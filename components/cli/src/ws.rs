//! Axum HTTP + WebSocket sync API.

use crate::hub::{DeliveryHub, SessionSubs};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
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
        .route("/v1/sync", get(ws_upgrade))
        .route("/v1/ingest", post(ingest))
        .with_state(AppState { hub })
}

pub async fn serve(bind_addr: &str, hub: Arc<DeliveryHub>) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    info!(%bind_addr, "sync API listening (ws /v1/sync, POST /v1/ingest)");
    axum::serve(listener, router(hub)).await?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
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
