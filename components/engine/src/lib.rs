//! Engine: CDS + Schema + Subscription Index + User States.
//!
//! Owns the in-memory sync core. Sources submit [`SourceUpdate`]s; the engine
//! applies them under schema authority, then fans changes into User State.

use cds::{Cds, Change, SourceUpdate};
use schema::SyncSchema;
use serde_json::{Map, Value};
use std::collections::HashMap;
use subscription_index::{SubId, Subscription, SubscriptionIndex};
use user_state::{Transition, UserState};

#[derive(Debug)]
pub struct Engine {
    pub cds: Cds,
    pub schema: SyncSchema,
    pub index: SubscriptionIndex,
    pub user_states: HashMap<SubId, UserState>,
}

impl Engine {
    pub fn new(cds: Cds, schema: SyncSchema) -> Self {
        Self {
            cds,
            schema,
            index: SubscriptionIndex::new(),
            user_states: HashMap::new(),
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
    /// Returns `(changed_field_names, transitions)`. `changed_field_names` is
    /// empty when the update was a no-op (stale / unchanged).
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

        let allowed = self
            .schema
            .fields_owned_by(&update.entity_type, &update.source);
        let change = self.cds.apply_source_update(update, &allowed);
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
        Ok((changed_fields, transitions))
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
        let entity_type = entity_type.into();
        if !self.schema.has_entity(&entity_type) && !self.cds.has_entity_type(&entity_type) {
            return Err(format!("unknown entity '{entity_type}'"));
        }

        let sub = self.index.subscribe(entity_type.clone(), where_eq);
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
      location: { source: http, mode: latest_value, ordering: sequence }
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
    fn apply_http_update_preserves_postgres_fields() {
        let mut engine = sample_engine();
        engine
            .subscribe("drivers", Map::new())
            .unwrap();
        let transitions = engine
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
        assert!(!transitions.1.is_empty());
        let state = engine.cds.get("drivers", "1").unwrap();
        assert_eq!(state.fields["name"], json!("Alice"));
        assert_eq!(state.fields["location"], json!({"lat": 1.0}));
    }
}
