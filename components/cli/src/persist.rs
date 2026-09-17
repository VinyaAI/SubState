//! Disk snapshot of CDS entities, subscriptions, and delta history.
//!
//! Postgres fields are still re-snapshotted on boot; this preserves Kafka/HTTP
//! fields and resume cursors across a process restart.

use anyhow::{Context, Result};
use cds::{EntityId, EntityState};
use delta::DeltaHistory;
use engine::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

const SNAPSHOT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedSubscription {
    pub id: String,
    pub entity_type: String,
    pub where_eq: Map<String, Value>,
    pub history: DeltaHistory,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SnapshotFile {
    version: u32,
    entities: Vec<(EntityId, EntityState)>,
    subscriptions: Vec<PersistedSubscription>,
}

pub fn default_snapshot_path() -> PathBuf {
    std::env::var("SUBSTATE_SNAPSHOT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./substate-snapshot.json"))
}

pub fn load(path: &Path) -> Result<Option<SnapshotFile>> {
    if !path.exists() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read snapshot {}", path.display()))?;
    let snap: SnapshotFile = serde_json::from_str(&text)
        .with_context(|| format!("parse snapshot {}", path.display()))?;
    if snap.version != SNAPSHOT_VERSION {
        warn!(
            version = snap.version,
            expected = SNAPSHOT_VERSION,
            "ignoring incompatible snapshot"
        );
        return Ok(None);
    }
    Ok(Some(snap))
}

pub fn save(path: &Path, engine: &Engine, histories: &HashMap<String, DeltaHistory>) -> Result<()> {
    let mut subscriptions = Vec::new();
    for (sub, _) in engine.subscriptions() {
        let history = histories
            .get(&sub.id)
            .cloned()
            .unwrap_or_else(DeltaHistory::new);
        subscriptions.push(PersistedSubscription {
            id: sub.id.clone(),
            entity_type: sub.entity_type.clone(),
            where_eq: sub.where_eq.clone(),
            history,
        });
    }
    let snap = SnapshotFile {
        version: SNAPSHOT_VERSION,
        entities: engine.cds.export_entities(),
        subscriptions,
    };
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let tmp = path.with_extension("json.tmp");
    let text = serde_json::to_string_pretty(&snap)?;
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Merge non-Postgres fields from a snapshot onto a freshly snapshotted CDS,
/// then restore subscriptions + histories.
pub fn restore_into(
    engine: &mut Engine,
    histories: &mut HashMap<String, DeltaHistory>,
    snap: SnapshotFile,
) {
    // Overlay Kafka/HTTP (and any) fields from the snapshot onto current CDS.
    for (id, state) in snap.entities {
        if let Some(existing) = engine.cds.get(&id.entity_type, &id.id).cloned() {
            let mut merged = existing;
            for (field, value) in state.fields {
                let meta = state.field_meta.get(&field);
                // Prefer snapshot field when it is not owned by postgres, or when
                // the live CDS does not yet have it.
                let source = meta.map(|m| m.source.as_str()).unwrap_or("");
                let is_postgres = source == "postgres"
                    || engine
                        .schema
                        .field_authority(&id.entity_type, &field)
                        == Some("postgres")
                        && source.is_empty();
                if !is_postgres || !merged.fields.contains_key(&field) {
                    merged.fields.insert(field.clone(), value);
                    if let Some(m) = meta {
                        merged.field_meta.insert(field, m.clone());
                    }
                }
            }
            engine.cds.insert(id, merged);
        } else {
            // Entity only known from streams — restore whole row.
            engine.cds.insert(id, state);
        }
    }

    histories.clear();
    // Clear live subscriptions before restore.
    let live_ids: Vec<String> = engine
        .subscriptions()
        .into_iter()
        .map(|(s, _)| s.id.clone())
        .collect();
    for id in live_ids {
        engine.unsubscribe(&id);
    }

    for persisted in snap.subscriptions {
        match engine.restore_subscription(
            persisted.id.clone(),
            persisted.entity_type,
            persisted.where_eq,
        ) {
            Ok((sub, _)) => {
                histories.insert(sub.id, persisted.history);
            }
            Err(err) => warn!(error = %err, sub = %persisted.id, "skip restoring subscription"),
        }
    }

    info!(
        entities = engine.cds.entity_count(),
        subscriptions = histories.len(),
        "restored SubState snapshot"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use cds::{Cds, EntityState, TableCatalog};
    use schema::SyncSchema;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn round_trip_snapshot_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snap.json");
        let schema = SyncSchema::from_yaml_str(
            r#"
entities:
  driver:
    identity: { field: id }
    sources:
      postgres: { type: postgres, table: drivers }
      http: { type: http }
    fields:
      id: { source: postgres }
      name: { source: postgres }
      location: { source: http, mode: latest_value }
"#,
        )
        .unwrap();
        let mut cds = Cds::new("public");
        cds.add_table(TableCatalog {
            name: "driver".into(),
            primary_key: vec!["id".into()],
            columns: vec!["id".into(), "name".into(), "location".into()],
            row_count: 1,
        });
        cds.insert(
            EntityId {
                entity_type: "driver".into(),
                id: "1".into(),
            },
            EntityState::from_fields(
                json!({"id": 1, "name": "Alice", "location": {"lat": 1.0}})
                    .as_object()
                    .cloned()
                    .unwrap(),
            ),
        );
        let mut engine = Engine::new(cds, schema);
        engine.subscribe("driver", Map::new()).unwrap();
        let mut histories = HashMap::new();
        histories.insert("sub_1".into(), DeltaHistory::new());
        save(&path, &engine, &histories).unwrap();
        let loaded = load(&path).unwrap().unwrap();
        assert_eq!(loaded.entities.len(), 1);
        assert_eq!(loaded.subscriptions.len(), 1);
    }
}
