//! Axum HTTP + WebSocket sync API.

use crate::hub::{DeliveryHub, SessionSubs};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use delta::Delta;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, warn};

#[derive(Clone)]
pub struct AppState {
    pub hub: Arc<DeliveryHub>,
}

pub fn router(hub: Arc<DeliveryHub>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/cds", get(cds_dump))
        .route("/v1/cds/{entity}", get(cds_list))
        .route("/v1/cds/{entity}/{id}", get(cds_get))
        .route("/v1/sync", get(ws_upgrade))
        .route("/v1/ingest", post(ingest))
        .with_state(AppState { hub })
}

pub async fn serve(bind_addr: &str, hub: Arc<DeliveryHub>) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    info!(%bind_addr, "sync API listening (GET /v1/cds, ws /v1/sync, POST /v1/ingest)");
    axum::serve(listener, router(hub)).await?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

#[derive(Debug, Deserialize)]
struct CdsListQuery {
    #[serde(default = "default_cds_limit")]
    limit: usize,
}

fn default_cds_limit() -> usize {
    20
}

fn entity_json(id: String, state: cds::EntityState) -> Value {
    let mut value = json!({
        "id": id,
        "fields": state.fields,
    });
    if !state.field_meta.is_empty() {
        value["field_meta"] = serde_json::to_value(state.field_meta).unwrap_or(Value::Null);
    }
    value
}

/// GET /v1/cds — catalog plus current entities (per-type limit, default 20, max 100).
async fn cds_dump(
    State(state): State<AppState>,
    Query(query): Query<CdsListQuery>,
) -> Json<Value> {
    let catalog = state.hub.cds_catalog().await;
    let entity_count = state.hub.cds_entity_count().await;
    let mut entities = Vec::new();
    for table in &catalog.tables {
        let (total, items) = state
            .hub
            .cds_list(&table.name, query.limit)
            .await
            .unwrap_or((0, Vec::new()));
        entities.push(json!({
            "name": table.name,
            "primary_key": table.primary_key,
            "columns": table.columns,
            "row_count": table.row_count,
            "total": total,
            "items": items
                .into_iter()
                .map(|(id, entity)| entity_json(id, entity))
                .collect::<Vec<_>>(),
        }));
    }
    Json(json!({
        "schema": catalog.schema,
        "entity_count": entity_count,
        "limit": query.limit.clamp(1, 100),
        "entities": entities,
        "skipped": catalog.skipped,
    }))
}

/// GET /v1/cds/:entity
async fn cds_list(
    State(state): State<AppState>,
    Path(entity): Path<String>,
    Query(query): Query<CdsListQuery>,
) -> impl IntoResponse {
    match state.hub.cds_list(&entity, query.limit).await {
        Ok((total, items)) => (
            StatusCode::OK,
            Json(json!({
                "entity": entity,
                "total": total,
                "limit": query.limit.clamp(1, 100),
                "items": items
                    .into_iter()
                    .map(|(id, entity)| entity_json(id, entity))
                    .collect::<Vec<_>>(),
            })),
        ),
        Err(message) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": message })),
        ),
    }
}

/// GET /v1/cds/:entity/:id
async fn cds_get(
    State(state): State<AppState>,
    Path((entity, id)): Path<(String, String)>,
) -> impl IntoResponse {
    match state.hub.cds_get(&entity, &id).await {
        Ok(Some(state)) => (
            StatusCode::OK,
            Json(json!({
                "entity": entity,
                "id": id,
                "fields": state.fields,
                "field_meta": state.field_meta,
            })),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": format!("entity {entity}:{id} not found"),
            })),
        ),
        Err(message) => (StatusCode::NOT_FOUND, Json(json!({ "error": message }))),
    }
}

#[derive(Debug, Deserialize)]
struct IngestRequest {
    source: String,
    entity_type: String,
    id: String,
    #[serde(default)]
    fields: Map<String, Value>,
    #[serde(default)]
    versions: std::collections::HashMap<String, u64>,
}

async fn ingest(
    State(state): State<AppState>,
    Json(body): Json<IngestRequest>,
) -> impl IntoResponse {
    let update = cds::SourceUpdate {
        source: body.source,
        entity_type: body.entity_type,
        id: body.id,
        fields: body.fields,
        versions: body.versions,
    };
    match state.hub.apply_and_publish(update).await {
        Ok(changed_fields) => {
            let accepted = !changed_fields.is_empty();
            (
                axum::http::StatusCode::OK,
                Json(json!({
                    "accepted": accepted,
                    "changed_fields": changed_fields,
                })),
            )
        }
        Err(message) => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({
                "accepted": false,
                "error": message,
            })),
        ),
    }
}

async fn ws_upgrade(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMessage {
    Subscribe {
        entity_type: String,
        #[serde(default, rename = "where")]
        filter: Map<String, Value>,
    },
    Resume {
        subscription: String,
        resume_after: u64,
    },
    Unsubscribe {
        subscription: String,
    },
    Ack {
        subscription: String,
        seq: u64,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMessage {
    Subscribed {
        subscription: String,
    },
    Snapshot {
        subscription: String,
        entities: Vec<delta::SnapshotEntity>,
    },
    Reset {
        subscription: String,
        reason: String,
    },
    Error {
        message: String,
    },
}

fn delta_json(delta: &Delta) -> Value {
    let mut value = json!({
        "type": "delta",
        "subscription": delta.subscription,
        "seq": delta.seq,
        "op": delta.op,
        "entity": delta.entity,
        "id": delta.id,
    });
    if let Some(fields) = &delta.fields {
        value["fields"] = Value::Object(fields.clone());
    }
    value
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut events = state.hub.subscribe_events();
    let mut session = SessionSubs::default();

    loop {
        tokio::select! {
            incoming = receiver.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<ClientMessage>(&text) {
                            Ok(msg) => {
                                if let Err(err) = handle_client_message(
                                    &state,
                                    &mut session,
                                    &mut sender,
                                    msg,
                                ).await {
                                    let _ = send_json(&mut sender, &ServerMessage::Error { message: err }).await;
                                }
                            }
                            Err(err) => {
                                let _ = send_json(
                                    &mut sender,
                                    &ServerMessage::Error {
                                        message: format!("invalid message: {err}"),
                                    },
                                ).await;
                            }
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        let _ = sender.send(Message::Pong(payload)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(err)) => {
                        warn!(error = %err, "websocket error");
                        break;
                    }
                }
            }
            event = events.recv() => {
                match event {
                    Ok(crate::hub::HubEvent::Delta(delta)) => {
                        if session.contains(&delta.subscription) {
                            let _ = sender
                                .send(Message::Text(delta_json(&delta).to_string().into()))
                                .await;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Drop lagged events; client can resume.
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}

async fn handle_client_message(
    state: &AppState,
    session: &mut SessionSubs,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    msg: ClientMessage,
) -> Result<(), String> {
    match msg {
        ClientMessage::Subscribe {
            entity_type,
            filter,
        } => {
            let (sub_id, entities) = state.hub.subscribe(entity_type, filter).await?;
            session.insert(sub_id.clone());
            send_json(
                sender,
                &ServerMessage::Subscribed {
                    subscription: sub_id.clone(),
                },
            )
            .await?;
            send_json(
                sender,
                &ServerMessage::Snapshot {
                    subscription: sub_id,
                    entities,
                },
            )
            .await?;
        }
        ClientMessage::Resume {
            subscription,
            resume_after,
        } => {
            if !state.hub.has_subscription(&subscription).await {
                return Err(format!("unknown subscription '{subscription}'"));
            }
            session.insert(subscription.clone());
            match state.hub.resume_since(&subscription, resume_after).await? {
                Ok(deltas) => {
                    for delta in deltas {
                        sender
                            .send(Message::Text(delta_json(&delta).to_string().into()))
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                }
                Err(_) => {
                    send_json(
                        sender,
                        &ServerMessage::Reset {
                            subscription: subscription.clone(),
                            reason: "history_expired".to_string(),
                        },
                    )
                    .await?;
                    let entities = state.hub.snapshot_for(&subscription).await?;
                    send_json(
                        sender,
                        &ServerMessage::Snapshot {
                            subscription,
                            entities,
                        },
                    )
                    .await?;
                }
            }
        }
        ClientMessage::Unsubscribe { subscription } => {
            session.remove(&subscription);
            if !state.hub.unsubscribe(&subscription).await {
                return Err(format!("unknown subscription '{subscription}'"));
            }
        }
        ClientMessage::Ack { subscription, seq } => {
            state.hub.ack(&subscription, seq).await?;
        }
    }
    Ok(())
}

async fn send_json(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    message: &ServerMessage,
) -> Result<(), String> {
    let value = serde_json::to_value(message).map_err(|e| e.to_string())?;
    sender
        .send(Message::Text(value.to_string().into()))
        .await
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cds::{EntityId, EntityState, TableCatalog};
    use engine::Engine;
    use schema::SyncSchema;
    use tokio::sync::RwLock;

    fn sample_hub() -> Arc<DeliveryHub> {
        let schema = SyncSchema::from_yaml_str(
            r#"
entities:
  driver:
    identity: { field: id }
    sources:
      postgres: { type: postgres, table: drivers }
    fields:
      id: { source: postgres }
      name: { source: postgres }
"#,
        )
        .unwrap();
        let mut cds = cds::Cds::new("public");
        cds.add_table(TableCatalog {
            name: "driver".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec!["id".to_string(), "name".to_string()],
            row_count: 1,
        });
        cds.insert(
            EntityId {
                entity_type: "driver".to_string(),
                id: "1".to_string(),
            },
            EntityState::from_fields(
                json!({"id": 1, "name": "Alice"})
                    .as_object()
                    .cloned()
                    .unwrap(),
            ),
        );
        Arc::new(DeliveryHub::new(Arc::new(RwLock::new(Engine::new(
            cds, schema,
        )))))
    }

    #[tokio::test]
    async fn cds_inspect_lists_known_entity() {
        let hub = sample_hub();
        let catalog = hub.cds_catalog().await;
        assert_eq!(catalog.tables[0].name, "driver");
        assert_eq!(hub.cds_entity_count().await, 1);
        let (total, items) = hub.cds_list("driver", 20).await.unwrap();
        assert_eq!(total, 1);
        assert_eq!(items[0].0, "1");
        assert_eq!(items[0].1.fields["name"], json!("Alice"));
        let got = hub.cds_get("driver", "1").await.unwrap().unwrap();
        assert_eq!(got.fields["name"], json!("Alice"));
        assert!(hub.cds_get("driver", "missing").await.unwrap().is_none());
        assert!(hub.cds_list("unknown", 10).await.is_err());
    }
}
