//! MongoDB adapter: poll collections into [`SourceUpdate`]s.
//!
//! Enabled when the sync schema has a `mongodb` source and `MONGO_URL` is set.
//! The schema `table` field holds the collection name.

use anyhow::{Context, Result};
use cds::SourceUpdate;
use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document};
use mongodb::{Client, Collection};
use schema::{SyncSchema, SOURCE_MONGODB};
use serde_json::{Map, Value};
use source::{Follow, SourceEvent};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub struct MongoPollFollow {
    pub url: String,
    pub database: String,
    pub poll_ms: u64,
    pub sync_schema: Arc<SyncSchema>,
}

impl Follow for MongoPollFollow {
    fn spawn(self: Box<Self>, tx: mpsc::Sender<SourceEvent>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let client = match Client::with_uri_str(&self.url).await {
                Ok(c) => c,
                Err(err) => {
                    warn!(error = %err, "mongo connect failed");
                    return;
                }
            };
            let db = client.database(&self.database);
            info!(
                database = %self.database,
                poll_ms = self.poll_ms,
                "mongo poll follow started"
            );
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(self.poll_ms.max(200)));
            loop {
                interval.tick().await;
                if let Err(err) = poll_once(&db, &self.sync_schema, &tx).await {
                    warn!(error = %err, "mongo poll cycle failed");
                }
            }
        })
    }
}

async fn poll_once(
    db: &mongodb::Database,
    sync_schema: &SyncSchema,
    tx: &mpsc::Sender<SourceEvent>,
) -> Result<()> {
    for (logical_name, entity) in &sync_schema.entities {
        let Some((source_id, source)) = entity
            .sources
            .iter()
            .find(|(_, s)| s.source_type == SOURCE_MONGODB)
        else {
            continue;
        };
        let Some(collection) = source.table.as_deref() else {
            continue;
        };
        let coll: Collection<Document> = db.collection(collection);
        let mut cursor = coll
            .find(Document::new())
            .await
            .with_context(|| format!("mongo find {collection}"))?;
        let owned = sync_schema.fields_owned_by(logical_name, source_id);
        let path_map = sync_schema.path_map(logical_name, source_id);
        while let Some(doc) = cursor.try_next().await? {
            let value = bson_to_json(Bson::Document(doc));
            if let Some(update) = project_mongo_doc(
                logical_name,
                source_id,
                entity.identity.field(),
                &owned,
                &value,
                &path_map,
            ) {
                if tx.send(SourceEvent::Upsert(update)).await.is_err() {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

/// Project a MongoDB JSON document onto owned logical fields.
pub fn project_mongo_doc(
    entity_type: &str,
    source_id: &str,
    identity_field: &str,
    owned_fields: &[&str],
    doc: &Value,
    path_map: &HashMap<String, String>,
) -> Option<SourceUpdate> {
    let object = doc.as_object()?;
    let id_key = path_map
        .get(identity_field)
        .map(String::as_str)
        .unwrap_or(identity_field);
    // Prefer explicit identity, then Mongo `_id`.
    let id_val = object
        .get(id_key)
        .or_else(|| object.get(identity_field))
        .or_else(|| object.get("_id"))?;
    let id = match id_val {
        Value::String(s) => s.clone(),
        Value::Object(oid) => oid
            .get("$oid")
            .and_then(|v| v.as_str())
            .unwrap_or(&id_val.to_string())
            .to_string(),
        Value::Number(n) => n.to_string(),
        other => other.to_string(),
    };
    let mut fields = Map::new();
    for key in owned_fields {
        let physical = path_map.get(*key).map(String::as_str).unwrap_or(*key);
        if let Some(value) = object.get(physical).or_else(|| object.get(*key)) {
            fields.insert((*key).to_string(), value.clone());
        }
    }
    if fields.is_empty() {
        return None;
    }
    Some(SourceUpdate {
        source: source_id.to_string(),
        entity_type: entity_type.to_string(),
        id,
        fields,
        versions: HashMap::new(),
    })
}

fn bson_to_json(bson: Bson) -> Value {
    serde_json::to_value(bson).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn projects_document() {
        let owned = ["id", "name"];
        let update = project_mongo_doc(
            "driver",
            "mongo",
            "id",
            &owned,
            &json!({"id": "abc", "name": "Ada", "extra": 1}),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(update.id, "abc");
        assert!(!update.fields.contains_key("extra"));
    }
}
