//! Engine: CDS + Schema + Subscription Index + User States.
//!
//! Owns the in-memory sync core. Sources submit [`SourceUpdate`]s; the engine
//! applies them under schema authority, then fans changes into User State.
//! Latest-value fields are coalesced for `flush_ms` before fan-out.

use cds::{Cds, Change, SourceUpdate};
use schema::{FieldMode, SyncSchema};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use subscription_index::{SubId, Subscription, SubscriptionIndex};
use user_state::{Transition, UserState};

/// Default coalesce window when `mode: latest_value` omits `flush_ms`.
pub const DEFAULT_LATEST_VALUE_FLUSH_MS: u64 = 100;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CoalesceKey {
    entity_type: String,
    id: String,
    field: String,
}

#[derive(Debug)]
struct PendingField {
    source: String,
    value: Value,
    version: u64,
    flush_at: Instant,
}

#[derive(Debug)]
pub struct Engine {
    pub cds: Cds,
    pub schema: SyncSchema,
    pub index: SubscriptionIndex,
    pub user_states: HashMap<SubId, UserState>,
    coalesce: HashMap<CoalesceKey, PendingField>,
}

impl Engine {
    pub fn new(cds: Cds, schema: SyncSchema) -> Self {
        Self {
            cds,
            schema,
            index: SubscriptionIndex::new(),
            user_states: HashMap::new(),
            coalesce: HashMap::new(),
        }
    }

    /// Apply CDS changes that were already written into `self.cds`.
    pub fn fanout(&mut self, changes: &[Change]) -> Vec<Transition> {
        let mut transitions = Vec::new();
        for change in changes {
            let candidates = self.index.candidates(change);
            for sub_id in candidates {
                let Some(sub) = self.index.get(&sub_id).cloned() else {
                    continue;
                };
                let Some(user_state) = self.user_states.get_mut(&sub_id) else {
                    continue;
                };
                if let Some(transition) = user_state.apply_change(&sub, change) {
                    transitions.push(transition);
                }
            }
        }
        transitions
    }

    /// Schema-aware merge of one source update, then fan-out.
    ///
    /// Transactional fields apply immediately. Latest-value fields buffer until
    /// [`Self::flush_coalesced`] (or an explicit flush of due entries).
    ///
    /// Returns `(changed_field_names, transitions)`. `changed_fields` includes
    /// fields that were accepted into CDS immediately; coalesced fields appear
    /// only after flush.
    pub fn apply_source_update(
        &mut self,
        update: SourceUpdate,
    ) -> Result<(Vec<String>, Vec<Transition>), String> {
        if !self.schema.has_entity(&update.entity_type) {
            return Err(format!("unknown entity '{}'", update.entity_type));
        }
        if self
            .schema
            .entity(&update.entity_type)
            .map(|e| e.sources.contains_key(&update.source))
            != Some(true)
        {
            return Err(format!(
                "unknown source '{}' for entity '{}'",
                update.source, update.entity_type
            ));
        }

        for field in update.fields.keys() {
            match self.schema.field_authority(&update.entity_type, field) {
                Some(owner) if owner == update.source => {}
                Some(owner) => {
                    return Err(format!(
                        "field '{field}' is owned by '{owner}', not '{}'",
                        update.source
                    ));
                }
                None => {
                    return Err(format!(
                        "unknown field '{field}' on entity '{}'",
                        update.entity_type
                    ));
                }
            }
        }

        let entity = self
            .schema
            .entity(&update.entity_type)
            .expect("entity checked above");

        let mut immediate_fields = Map::new();
        let mut immediate_versions = HashMap::new();
        let mut buffered_fields = Vec::new();
        let now = Instant::now();

        for (field, value) in update.fields {
            let field_def = entity.fields.get(&field).expect("authority checked");
            let version = update.versions.get(&field).copied().unwrap_or_else(|| {
                self.cds
                    .get(&update.entity_type, &update.id)
                    .and_then(|s| s.field_meta.get(&field).map(|m| m.version.saturating_add(1)))
                    .unwrap_or(1)
            });

            if field_def.mode == FieldMode::LatestValue {
                let flush_ms = field_def
                    .flush_ms
                    .unwrap_or(DEFAULT_LATEST_VALUE_FLUSH_MS);
                let key = CoalesceKey {
                    entity_type: update.entity_type.clone(),
                    id: update.id.clone(),
                    field: field.clone(),
                };
                if let Some(existing) = self.coalesce.get(&key) {
                    if version <= existing.version {
                        continue;
                    }
                }
                self.coalesce.insert(
                    key,
                    PendingField {
                        source: update.source.clone(),
                        value,
                        version,
                        flush_at: now + Duration::from_millis(flush_ms),
                    },
                );
                buffered_fields.push(field);
            } else {
                immediate_fields.insert(field.clone(), value);
                immediate_versions.insert(field, version);
            }
        }

        if immediate_fields.is_empty() {
            return Ok((buffered_fields, Vec::new()));
        }

        let allowed = self
            .schema
            .fields_owned_by(&update.entity_type, &update.source);
        let change = self.cds.apply_source_update(
            SourceUpdate {
                source: update.source,
                entity_type: update.entity_type,
                id: update.id,
                fields: immediate_fields,
                versions: immediate_versions,
            },
            &allowed,
        );
        let changed_fields = match &change {
            Some(change) => match &change.kind {
                cds::ChangeKind::Insert => change
                    .state
                    .as_ref()
                    .map(|s| s.fields.keys().cloned().collect())
                    .unwrap_or_default(),
                cds::ChangeKind::Update { fields } => fields.clone(),
                cds::ChangeKind::Delete => Vec::new(),
            },
            None => Vec::new(),
        };
        let transitions = match change {
            Some(change) => self.fanout(&[change]),
            None => Vec::new(),
        };
        let mut all_changed = changed_fields;
        all_changed.extend(buffered_fields);
        Ok((all_changed, transitions))
    }

    /// Flush coalesced latest-value fields whose window has elapsed.
    pub fn flush_coalesced(&mut self, now: Instant) -> Vec<Transition> {
        let due: Vec<CoalesceKey> = self
            .coalesce
            .iter()
            .filter(|(_, pending)| pending.flush_at <= now)
            .map(|(k, _)| k.clone())
            .collect();
        if due.is_empty() {
            return Vec::new();
        }

        // Group by (source, entity_type, id) so one CDS write covers many fields.
        let mut groups: HashMap<(String, String, String), SourceUpdate> = HashMap::new();
        for key in due {
            let Some(pending) = self.coalesce.remove(&key) else {
                continue;
            };
            let group_key = (
                pending.source.clone(),
                key.entity_type.clone(),
                key.id.clone(),
            );
            let entry = groups.entry(group_key).or_insert_with(|| SourceUpdate {
                source: pending.source.clone(),
                entity_type: key.entity_type.clone(),
                id: key.id.clone(),
                fields: Map::new(),
                versions: HashMap::new(),
            });
            entry.fields.insert(key.field.clone(), pending.value);
            entry.versions.insert(key.field, pending.version);
        }

        let mut transitions = Vec::new();
        for update in groups.into_values() {
            let allowed = self
                .schema
                .fields_owned_by(&update.entity_type, &update.source);
            if let Some(change) = self.cds.apply_source_update(update, &allowed) {
                transitions.extend(self.fanout(&[change]));
            }
        }
        if !transitions.is_empty() {
            tracing::trace!(count = transitions.len(), "flushed coalesced latest-value fields");
        }
        transitions
    }

    /// Instant when the next coalesced field should flush, if any.
    pub fn next_coalesce_deadline(&self) -> Option<Instant> {
        self.coalesce.values().map(|p| p.flush_at).min()
    }

    /// Apply many updates (one poll cycle / batch ingest).
    pub fn apply_source_updates(
        &mut self,
        updates: Vec<SourceUpdate>,
    ) -> Result<Vec<Transition>, String> {
        let mut transitions = Vec::new();
        for update in updates {
            let (_, t) = self.apply_source_update(update)?;
            transitions.extend(t);
        }
        Ok(transitions)
    }

    /// Delete an entity (e.g. postgres identity row disappeared).
    pub fn remove_entity(&mut self, entity_type: &str, id: &str) -> Vec<Transition> {
        use cds::EntityId;
        // Drop pending coalesce for this entity.
        self.coalesce
            .retain(|k, _| !(k.entity_type == entity_type && k.id == id));
        let change = self.cds.remove(&EntityId {
            entity_type: entity_type.to_string(),
            id: id.to_string(),
        });
        match change {
            Some(change) => self.fanout(&[change]),
            None => Vec::new(),
        }
    }

    /// Register a subscription, materialize initial User State from the CDS.
    pub fn subscribe(
        &mut self,
        entity_type: impl Into<String>,
        where_eq: Map<String, Value>,
    ) -> Result<(Subscription, UserState), String> {
        self.subscribe_inner(None, entity_type.into(), where_eq)
    }

    /// Restore a subscription with a known id (disk snapshot).
    pub fn restore_subscription(
        &mut self,
        id: impl Into<String>,
        entity_type: impl Into<String>,
        where_eq: Map<String, Value>,
    ) -> Result<(Subscription, UserState), String> {
        self.subscribe_inner(Some(id.into()), entity_type.into(), where_eq)
    }

    fn subscribe_inner(
        &mut self,
        id: Option<String>,
        entity_type: String,
        where_eq: Map<String, Value>,
    ) -> Result<(Subscription, UserState), String> {
        if !self.schema.has_entity(&entity_type) && !self.cds.has_entity_type(&entity_type) {
            return Err(format!("unknown entity '{entity_type}'"));
        }

        let sub = match id {
            Some(id) => self.index.subscribe_with_id(id, entity_type.clone(), where_eq),
            None => self.index.subscribe(entity_type.clone(), where_eq),
        };
        let mut user_state = UserState::new(sub.id.clone());
        for (id, state) in self.cds.iter_type(&entity_type) {
            if SubscriptionIndex::matches(&sub, state) {
                user_state.entities.insert(id.clone(), state.clone());
            }
        }
        self.user_states.insert(sub.id.clone(), user_state.clone());
        Ok((sub, user_state))
    }

    pub fn unsubscribe(&mut self, id: &str) -> bool {
        let removed = self.index.unsubscribe(id);
        self.user_states.remove(id);
        removed
    }

    pub fn user_state(&self, id: &str) -> Option<&UserState> {
        self.user_states.get(id)
    }

    pub fn subscriptions(&self) -> Vec<(&Subscription, usize)> {
        self.index
            .list()
            .into_iter()
            .map(|sub| {
                let count = self
                    .user_states
                    .get(&sub.id)
                    .map(UserState::entity_count)
                    .unwrap_or(0);
                (sub, count)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cds::{EntityId, EntityState, TableCatalog};
    use serde_json::json;

    fn sample_schema() -> SyncSchema {
        SyncSchema::from_yaml_str(
            r#"
entities:
  drivers:
    identity: { field: id }
    sources:
      postgres: { type: postgres, table: drivers }
      http: { type: http }
    fields:
      id: { source: postgres }
      name: { source: postgres }
      status: { source: postgres }
      location: { source: http, mode: latest_value, ordering: sequence, flush_ms: 50 }
"#,
        )
        .unwrap()
    }

    fn sample_engine() -> Engine {
        let mut cds = Cds::new("public");
        cds.add_table(TableCatalog {
            name: "drivers".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec![
                "id".to_string(),
                "name".to_string(),
                "status".to_string(),
                "location".to_string(),
            ],
            row_count: 1,
        });
        cds.insert(
            EntityId {
                entity_type: "drivers".to_string(),
                id: "1".to_string(),
            },
            EntityState::from_fields(
                json!({"id": 1, "name": "Alice", "status": "available"})
                    .as_object()
                    .cloned()
                    .unwrap(),
            ),
        );
        Engine::new(cds, sample_schema())
    }

    #[test]
    fn subscribe_materializes_matches() {
        let mut engine = sample_engine();
        let (sub, us) = engine
            .subscribe(
                "drivers",
                json!({"status": "available"}).as_object().cloned().unwrap(),
            )
            .unwrap();
        assert_eq!(sub.id, "sub_1");
        assert_eq!(us.entity_count(), 1);
    }

    #[test]
    fn latest_value_coalesces_until_flush() {
        let mut engine = sample_engine();
        engine.subscribe("drivers", Map::new()).unwrap();
        let (changed, transitions) = engine
            .apply_source_update(SourceUpdate {
                source: "http".into(),
                entity_type: "drivers".into(),
                id: "1".into(),
                fields: json!({"location": {"lat": 1.0}})
                    .as_object()
                    .cloned()
                    .unwrap(),
                versions: HashMap::from([("location".into(), 1u64)]),
            })
            .unwrap();
        assert_eq!(changed, vec!["location".to_string()]);
        assert!(transitions.is_empty());
        assert!(engine.cds.get("drivers", "1").unwrap().fields.get("location").is_none());

        // Newer value before flush replaces pending.
        let _ = engine
            .apply_source_update(SourceUpdate {
                source: "http".into(),
                entity_type: "drivers".into(),
                id: "1".into(),
                fields: json!({"location": {"lat": 2.0}})
                    .as_object()
                    .cloned()
                    .unwrap(),
                versions: HashMap::from([("location".into(), 2u64)]),
            })
            .unwrap();

        let flushed = engine.flush_coalesced(Instant::now() + Duration::from_millis(100));
        assert!(!flushed.is_empty());
        assert_eq!(
            engine.cds.get("drivers", "1").unwrap().fields["location"],
            json!({"lat": 2.0})
        );
    }

    #[test]
    fn apply_http_update_preserves_postgres_fields() {
        let mut engine = sample_engine();
        engine.subscribe("drivers", Map::new()).unwrap();
        let _ = engine
            .apply_source_update(SourceUpdate {
                source: "http".into(),
                entity_type: "drivers".into(),
                id: "1".into(),
                fields: json!({"location": {"lat": 1.0}})
                    .as_object()
                    .cloned()
                    .unwrap(),
                versions: HashMap::from([("location".into(), 1u64)]),
            })
            .unwrap();
        let _ = engine.flush_coalesced(Instant::now() + Duration::from_millis(100));
        let state = engine.cds.get("drivers", "1").unwrap();
        assert_eq!(state.fields["name"], json!("Alice"));
        assert_eq!(state.fields["location"], json!({"lat": 1.0}));
    }
}
