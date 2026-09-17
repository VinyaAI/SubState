//! In-memory Current Database State (CDS).
//!
//! Logical entities hold merged fields from multiple sources. Per-field
//! metadata tracks authority (`source`) and a source-local `version` so
//! updates from one backend cannot wipe another.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Identity of one entity inside the CDS.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityId {
    /// Logical entity type from the sync schema, e.g. `"driver"`.
    pub entity_type: String,
    /// Primary-key / identity value as a string, e.g. `"728"`.
    pub id: String,
}

/// Per-field authority and source-local version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldMeta {
    pub source: String,
    pub version: u64,
}

/// Current field values for one entity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct EntityState {
    pub fields: Map<String, Value>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_meta: HashMap<String, FieldMeta>,
}

impl EntityState {
    pub fn from_fields(fields: Map<String, Value>) -> Self {
        Self {
            fields,
            field_meta: HashMap::new(),
        }
    }
}

/// Metadata for one successfully loaded logical entity type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCatalog {
    pub name: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<String>,
    pub row_count: usize,
}

/// A table we discovered but chose not to load.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkippedTable {
    pub name: String,
    pub reason: String,
}

/// Lightweight catalog shown by the `tables` shell command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalog {
    pub schema: String,
    pub tables: Vec<TableCatalog>,
    pub skipped: Vec<SkippedTable>,
}

/// What changed inside the CDS after a merge/remove.
#[derive(Debug, Clone, PartialEq)]
pub struct Change {
    pub entity_type: String,
    pub id: String,
    pub kind: ChangeKind,
    /// State after insert/update, or last known state on delete.
    pub state: Option<EntityState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeKind {
    Insert,
    Update { fields: Vec<String> },
    Delete,
}

/// One source's proposed field patch (already authority-filtered by the caller,
/// or filtered inside [`Cds::apply_source_update`] via `allowed_fields`).
#[derive(Debug, Clone)]
pub struct SourceUpdate {
    pub source: String,
    pub entity_type: String,
    pub id: String,
    pub fields: Map<String, Value>,
    /// Per-field source-local versions. Missing keys default to `stored+1` / `1`.
    pub versions: HashMap<String, u64>,
}

/// The snapshot itself: every loaded entity plus a small catalog for inspection.
#[derive(Debug)]
pub struct Cds {
    entities: HashMap<EntityId, EntityState>,
    catalog: Catalog,
}

impl Cds {
    /// Create an empty CDS for the given Postgres schema name (catalog label).
    pub fn new(schema: impl Into<String>) -> Self {
        Self {
            entities: HashMap::new(),
            catalog: Catalog {
                schema: schema.into(),
                tables: Vec::new(),
                skipped: Vec::new(),
            },
        }
    }

    /// Store or replace one entity without emitting a Change (test/bootstrap helper).
    pub fn insert(&mut self, id: EntityId, state: EntityState) {
        self.entities.insert(id, state);
    }

    /// Insert or replace an entire entity (legacy full-row path). Prefer
    /// [`Self::apply_source_update`] for multi-source merges.
    pub fn upsert(&mut self, id: EntityId, state: EntityState) -> Option<Change> {
        match self.entities.get(&id) {
            None => {
                let change = Change {
                    entity_type: id.entity_type.clone(),
                    id: id.id.clone(),
                    kind: ChangeKind::Insert,
                    state: Some(state.clone()),
                };
                self.entities.insert(id.clone(), state);
                self.bump_row_count(&id.entity_type, 1);
                Some(change)
            }
            Some(existing) if existing.fields == state.fields => None,
            Some(existing) => {
                let fields = field_diff(&existing.fields, &state.fields);
                if fields.is_empty() {
                    return None;
                }
                let change = Change {
                    entity_type: id.entity_type.clone(),
                    id: id.id.clone(),
                    kind: ChangeKind::Update { fields },
                    state: Some(state.clone()),
                };
                self.entities.insert(id, state);
                Some(change)
            }
        }
    }

    /// Merge fields from one source into an entity.
    ///
    /// - Only keys listed in `allowed_fields` are considered (authority filter).
    /// - A field is accepted when its version is greater than the stored version
    ///   (or there is no stored meta yet).
    /// - Fields owned by other sources are never removed.
    pub fn apply_source_update(
        &mut self,
        update: SourceUpdate,
        allowed_fields: &[&str],
    ) -> Option<Change> {
        let allowed: std::collections::HashSet<&str> = allowed_fields.iter().copied().collect();
        let id = EntityId {
            entity_type: update.entity_type.clone(),
            id: update.id.clone(),
        };

        let mut accepted: Map<String, Value> = Map::new();
        let mut accepted_meta: HashMap<String, FieldMeta> = HashMap::new();

        let existing = self.entities.get(&id);
        for (field, value) in &update.fields {
            if !allowed.contains(field.as_str()) {
                continue;
            }
            let new_version = update
                .versions
                .get(field)
                .copied()
                .unwrap_or_else(|| {
                    existing
                        .and_then(|e| e.field_meta.get(field))
                        .map(|m| m.version.saturating_add(1))
                        .unwrap_or(1)
                });

            if let Some(meta) = existing.and_then(|e| e.field_meta.get(field)) {
                if new_version <= meta.version {
                    continue;
                }
            }

            let value_changed = existing
                .and_then(|e| e.fields.get(field))
                .map(|old| old != value)
                .unwrap_or(true);
            if !value_changed && existing.and_then(|e| e.field_meta.get(field)).is_some() {
                // Same value and already tracked — still bump meta if version advanced.
                if existing
                    .and_then(|e| e.field_meta.get(field))
                    .map(|m| m.version)
                    == Some(new_version)
                {
                    continue;
                }
            }

            accepted.insert(field.clone(), value.clone());
            accepted_meta.insert(
                field.clone(),
                FieldMeta {
                    source: update.source.clone(),
                    version: new_version,
                },
            );
        }

        if accepted.is_empty() {
            return None;
        }

        match existing {
            None => {
                let state = EntityState {
                    fields: accepted.clone(),
                    field_meta: accepted_meta,
                };
                let change = Change {
                    entity_type: id.entity_type.clone(),
                    id: id.id.clone(),
                    kind: ChangeKind::Insert,
                    state: Some(state.clone()),
                };
                self.entities.insert(id.clone(), state);
                self.bump_row_count(&id.entity_type, 1);
                Some(change)
            }
            Some(existing_state) => {
                let mut changed_names: Vec<String> = Vec::new();
                let mut new_state = existing_state.clone();
                for (field, value) in accepted {
                    let value_changed = new_state.fields.get(&field) != Some(&value);
                    new_state.fields.insert(field.clone(), value);
                    if let Some(meta) = accepted_meta.remove(&field) {
                        new_state.field_meta.insert(field.clone(), meta);
                    }
                    if value_changed {
                        changed_names.push(field);
                    }
                }
                if changed_names.is_empty() {
                    // Version-only bumps with identical values — still store meta.
                    self.entities.insert(id, new_state);
                    return None;
                }
                changed_names.sort();
                let change = Change {
                    entity_type: id.entity_type.clone(),
                    id: id.id.clone(),
                    kind: ChangeKind::Update {
                        fields: changed_names,
                    },
                    state: Some(new_state.clone()),
                };
                self.entities.insert(id, new_state);
                Some(change)
            }
        }
    }

    /// Remove an entity if present.
    pub fn remove(&mut self, id: &EntityId) -> Option<Change> {
        let state = self.entities.remove(id)?;
        self.bump_row_count(&id.entity_type, -1);
        Some(Change {
            entity_type: id.entity_type.clone(),
            id: id.id.clone(),
            kind: ChangeKind::Delete,
            state: Some(state),
        })
    }

    pub fn ids_for_type(&self, entity_type: &str) -> Vec<EntityId> {
        self.entities
            .keys()
            .filter(|id| id.entity_type == entity_type)
            .cloned()
            .collect()
    }

    pub fn iter_type(
        &self,
        entity_type: &str,
    ) -> impl Iterator<Item = (&EntityId, &EntityState)> + '_ {
        let entity_type = entity_type.to_string();
        self.entities
            .iter()
            .filter(move |(id, _)| id.entity_type == entity_type)
    }

    pub fn add_table(&mut self, table: TableCatalog) {
        if let Some(existing) = self
            .catalog
            .tables
            .iter_mut()
            .find(|t| t.name == table.name)
        {
            *existing = table;
        } else {
            self.catalog.tables.push(table);
        }
    }

    pub fn skip(&mut self, name: impl Into<String>, reason: impl Into<String>) {
        self.catalog.skipped.push(SkippedTable {
            name: name.into(),
            reason: reason.into(),
        });
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn has_entity_type(&self, entity_type: &str) -> bool {
        self.catalog
            .tables
            .iter()
            .any(|table| table.name == entity_type)
    }

    pub fn get(&self, entity_type: &str, id: &str) -> Option<&EntityState> {
        self.entities.get(&EntityId {
            entity_type: entity_type.to_string(),
            id: id.to_string(),
        })
    }

    pub fn list(&self, entity_type: &str, limit: usize) -> (usize, Vec<(&EntityId, &EntityState)>) {
        let mut matches: Vec<_> = self
            .entities
            .iter()
            .filter(|(id, _)| id.entity_type == entity_type)
            .collect();
        matches.sort_by(|a, b| a.0.id.cmp(&b.0.id));
        let total = matches.len();
        let items = matches.into_iter().take(limit).collect();
        (total, items)
    }

    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }

    /// Export all entities for persistence (order not guaranteed).
    pub fn export_entities(&self) -> Vec<(EntityId, EntityState)> {
        self.entities
            .iter()
            .map(|(id, state)| (id.clone(), state.clone()))
            .collect()
    }

    /// Replace entity map from a persisted snapshot. Catalog labels stay;
    /// row counts are recomputed.
    pub fn import_entities(&mut self, entities: Vec<(EntityId, EntityState)>) {
        self.entities.clear();
        for (id, state) in entities {
            self.entities.insert(id, state);
        }
        for table in &mut self.catalog.tables {
            table.row_count = self
                .entities
                .keys()
                .filter(|id| id.entity_type == table.name)
                .count();
        }
    }

    fn bump_row_count(&mut self, entity_type: &str, delta: i64) {
        if let Some(table) = self
            .catalog
            .tables
            .iter_mut()
            .find(|table| table.name == entity_type)
        {
            if delta >= 0 {
                table.row_count = table.row_count.saturating_add(delta as usize);
            } else {
                table.row_count = table.row_count.saturating_sub((-delta) as usize);
            }
        }
    }
}

fn field_diff(before: &Map<String, Value>, after: &Map<String, Value>) -> Vec<String> {
    let mut names: Vec<String> = before
        .keys()
        .chain(after.keys())
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .filter(|key| before.get(key) != after.get(key))
        .collect();
    names.sort();
    names
}

/// Build the CDS entity id string from a row's primary-key columns.
pub fn entity_id(primary_key: &[String], row: &Map<String, Value>) -> String {
    if primary_key.len() == 1 {
        return json_to_id_part(row.get(&primary_key[0]).unwrap_or(&Value::Null));
    }

    primary_key
        .iter()
        .map(|column| {
            format!(
                "{}={}",
                column,
                json_to_id_part(row.get(column).unwrap_or(&Value::Null))
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn json_to_id_part(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn object(value: serde_json::Value) -> Map<String, serde_json::Value> {
        value.as_object().cloned().expect("object")
    }

    fn drivers_cds() -> Cds {
        let mut cds = Cds::new("public");
        cds.add_table(TableCatalog {
            name: "driver".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec![
                "id".to_string(),
                "name".to_string(),
                "status".to_string(),
                "location".to_string(),
            ],
            row_count: 0,
        });
        cds
    }

    #[test]
    fn single_primary_key_uses_raw_value() {
        let row = object(json!({"id": 728, "name": "Alice"}));
        assert_eq!(entity_id(&["id".to_string()], &row), "728");
    }

    #[test]
    fn composite_primary_key_joins_columns() {
        let row = object(json!({"org": 1, "id": 42}));
        assert_eq!(
            entity_id(&["org".to_string(), "id".to_string()], &row),
            "org=1|id=42"
        );
    }

    #[test]
    fn insert_get_and_list_are_stable() {
        let mut cds = drivers_cds();
        cds.insert(
            EntityId {
                entity_type: "driver".to_string(),
                id: "2".to_string(),
            },
            EntityState::from_fields(object(json!({"id": 2, "name": "Bob"}))),
        );
        cds.insert(
            EntityId {
                entity_type: "driver".to_string(),
                id: "1".to_string(),
            },
            EntityState::from_fields(object(json!({"id": 1, "name": "Alice"}))),
        );
        cds.catalog.tables[0].row_count = 2;

        assert!(cds.has_entity_type("driver"));
        assert_eq!(
            cds.get("driver", "1")
                .map(|state| state.fields["name"].clone()),
            Some(json!("Alice"))
        );

        let (total, items) = cds.list("driver", 100);
        assert_eq!(total, 2);
        assert_eq!(items[0].0.id, "1");
        assert_eq!(items[1].0.id, "2");
    }

    #[test]
    fn upsert_insert_update_noop_and_remove() {
        let mut cds = drivers_cds();
        let id = EntityId {
            entity_type: "driver".to_string(),
            id: "1".to_string(),
        };

        let insert = cds
            .upsert(
                id.clone(),
                EntityState::from_fields(object(json!({"id": 1, "name": "Alice"}))),
            )
            .expect("insert");
        assert_eq!(insert.kind, ChangeKind::Insert);
        assert_eq!(cds.catalog().tables[0].row_count, 1);

        assert!(cds
            .upsert(
                id.clone(),
                EntityState::from_fields(object(json!({"id": 1, "name": "Alice"}))),
            )
            .is_none());

        let update = cds
            .upsert(
                id.clone(),
                EntityState::from_fields(object(json!({"id": 1, "name": "Alicia"}))),
            )
            .expect("update");
        match update.kind {
            ChangeKind::Update { fields } => assert_eq!(fields, vec!["name".to_string()]),
            other => panic!("expected update, got {other:?}"),
        }

        let delete = cds.remove(&id).expect("delete");
        assert_eq!(delete.kind, ChangeKind::Delete);
        assert_eq!(cds.catalog().tables[0].row_count, 0);
    }

    #[test]
    fn source_merge_preserves_other_fields_and_rejects_stale() {
        let mut cds = drivers_cds();
        let pg_fields = ["id", "name", "status"];
        let http_fields = ["location"];

        let insert = cds
            .apply_source_update(
                SourceUpdate {
                    source: "postgres".into(),
                    entity_type: "driver".into(),
                    id: "1".into(),
                    fields: object(json!({"id": 1, "name": "Alice", "status": "available"})),
                    versions: HashMap::new(),
                },
                &pg_fields,
            )
            .expect("pg insert");
        assert_eq!(insert.kind, ChangeKind::Insert);

        cds.apply_source_update(
            SourceUpdate {
                source: "http".into(),
                entity_type: "driver".into(),
                id: "1".into(),
                fields: object(json!({"location": {"lat": 1.0, "lng": 2.0}})),
                versions: HashMap::from([("location".into(), 10u64)]),
            },
            &http_fields,
        )
        .expect("http insert");

        // Postgres name change must not wipe location.
        cds.apply_source_update(
            SourceUpdate {
                source: "postgres".into(),
                entity_type: "driver".into(),
                id: "1".into(),
                fields: object(json!({"id": 1, "name": "Alicia", "status": "available"})),
                versions: HashMap::new(),
            },
            &pg_fields,
        )
        .expect("pg update");

        let state = cds.get("driver", "1").unwrap();
        assert_eq!(state.fields["name"], json!("Alicia"));
        assert_eq!(state.fields["location"], json!({"lat": 1.0, "lng": 2.0}));

        // Stale http version rejected.
        assert!(cds
            .apply_source_update(
                SourceUpdate {
                    source: "http".into(),
                    entity_type: "driver".into(),
                    id: "1".into(),
                    fields: object(json!({"location": {"lat": 9.0, "lng": 9.0}})),
                    versions: HashMap::from([("location".into(), 5u64)]),
                },
                &http_fields,
            )
            .is_none());
        assert_eq!(
            cds.get("driver", "1").unwrap().fields["location"],
            json!({"lat": 1.0, "lng": 2.0})
        );

        // Newer version accepted.
        cds.apply_source_update(
            SourceUpdate {
                source: "http".into(),
                entity_type: "driver".into(),
                id: "1".into(),
                fields: object(json!({"location": {"lat": 3.0, "lng": 4.0}})),
                versions: HashMap::from([("location".into(), 11u64)]),
            },
            &http_fields,
        )
        .expect("http newer");
        assert_eq!(
            cds.get("driver", "1").unwrap().fields["location"],
            json!({"lat": 3.0, "lng": 4.0})
        );

        // Wrong-source fields ignored (location not in pg allowed list).
        assert!(cds
            .apply_source_update(
                SourceUpdate {
                    source: "postgres".into(),
                    entity_type: "driver".into(),
                    id: "1".into(),
                    fields: object(json!({"location": {"lat": 0.0}})),
                    versions: HashMap::new(),
                },
                &pg_fields,
            )
            .is_none());
    }
}
