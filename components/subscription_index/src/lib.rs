//! Subscription Index: who cares about which CDS fields?
//!
//! v1 is coarse and equality-only:
//! - subscribe to an entity type with `where field=value` filters (AND)
//! - field dependency map: (entity_type, field) → subscription ids
//! - on Change, return candidate subscription ids to re-check

use cds::{Change, EntityState};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};

pub type SubId = String;

#[derive(Debug, Clone)]
pub struct Subscription {
    pub id: SubId,
    pub entity_type: String,
    /// Equality filters; every key must match the entity fields.
    pub where_eq: Map<String, Value>,
}

#[derive(Debug, Default)]
pub struct SubscriptionIndex {
    by_id: HashMap<SubId, Subscription>,
    /// Coarse field-level dependency index.
    by_field: HashMap<(String, String), Vec<SubId>>,
    /// All subscription ids for an entity type (used on Insert/Delete).
    by_type: HashMap<String, Vec<SubId>>,
    next_id: u64,
}

impl SubscriptionIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(&mut self, entity_type: impl Into<String>, where_eq: Map<String, Value>) -> Subscription {
        self.next_id += 1;
        let id = format!("sub_{}", self.next_id);
        let entity_type = entity_type.into();
        let sub = Subscription {
            id: id.clone(),
            entity_type: entity_type.clone(),
            where_eq: where_eq.clone(),
        };

        self.by_type
            .entry(entity_type.clone())
            .or_default()
            .push(id.clone());

        // Index every filter field so updates to those fields can find this sub.
        // Also index a sentinel for empty-where (match-all) via by_type only.
        for field in where_eq.keys() {
            self.by_field
                .entry((entity_type.clone(), field.clone()))
                .or_default()
                .push(id.clone());
        }

        self.by_id.insert(id, sub.clone());
        sub
    }

    pub fn unsubscribe(&mut self, id: &str) -> bool {
        let Some(sub) = self.by_id.remove(id) else {
            return false;
        };

        if let Some(list) = self.by_type.get_mut(&sub.entity_type) {
            list.retain(|existing| existing != id);
        }
        for field in sub.where_eq.keys() {
            if let Some(list) = self.by_field.get_mut(&(sub.entity_type.clone(), field.clone())) {
                list.retain(|existing| existing != id);
            }
        }
        true
    }

    pub fn get(&self, id: &str) -> Option<&Subscription> {
        self.by_id.get(id)
    }

    pub fn list(&self) -> Vec<&Subscription> {
        let mut subs: Vec<_> = self.by_id.values().collect();
        subs.sort_by(|a, b| a.id.cmp(&b.id));
        subs
    }

    /// Candidate subscription ids that may be affected by this CDS change.
    ///
    /// Updates notify every subscription on the entity type — not only those
    /// whose filter fields changed. A filtered sub still needs patches when a
    /// non-filter field changes on an entity it already holds (User State
    /// decides Add / Update / Remove / ignore).
    pub fn candidates(&self, change: &Change) -> Vec<SubId> {
        let mut ids: HashSet<SubId> = HashSet::new();

        if let Some(list) = self.by_type.get(&change.entity_type) {
            ids.extend(list.iter().cloned());
        }

        let mut out: Vec<_> = ids.into_iter().collect();
        out.sort();
        out
    }

    /// True when every where clause equals the entity's field value.
    pub fn matches(sub: &Subscription, state: &EntityState) -> bool {
        sub.where_eq.iter().all(|(key, expected)| {
            state.fields.get(key).map(|actual| actual == expected).unwrap_or(false)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cds::{ChangeKind, EntityState};
    use serde_json::json;

    fn state(status: &str, region: &str) -> EntityState {
        EntityState::from_fields(
            json!({"status": status, "region": region})
                .as_object()
                .cloned()
                .unwrap(),
        )
    }

    #[test]
    fn matches_equality_and() {
        let mut index = SubscriptionIndex::new();
        let sub = index.subscribe(
            "drivers",
            json!({"status": "available", "region": "nashville"})
                .as_object()
                .cloned()
                .unwrap(),
        );
        assert!(SubscriptionIndex::matches(
            &sub,
            &state("available", "nashville")
        ));
        assert!(!SubscriptionIndex::matches(
            &sub,
            &state("busy", "nashville")
        ));
    }

    #[test]
    fn candidates_for_insert_and_update() {
        let mut index = SubscriptionIndex::new();
        let sub = index.subscribe(
            "drivers",
            json!({"status": "available"}).as_object().cloned().unwrap(),
        );

        let insert = Change {
            entity_type: "drivers".to_string(),
            id: "1".to_string(),
            kind: ChangeKind::Insert,
            state: None,
        };
        assert_eq!(index.candidates(&insert), vec![sub.id.clone()]);

        let update_status = Change {
            entity_type: "drivers".to_string(),
            id: "1".to_string(),
            kind: ChangeKind::Update {
                fields: vec!["status".to_string()],
            },
            state: None,
        };
        assert_eq!(index.candidates(&update_status), vec![sub.id.clone()]);

        // Non-filter field updates must still wake filtered subscriptions so
        // in-membership entities get UPDATE patches (e.g. dob while filtering on name).
        let update_name = Change {
            entity_type: "drivers".to_string(),
            id: "1".to_string(),
            kind: ChangeKind::Update {
                fields: vec!["name".to_string()],
            },
            state: None,
        };
        assert_eq!(index.candidates(&update_name), vec![sub.id.clone()]);

        let other_type = Change {
            entity_type: "jobs".to_string(),
            id: "1".to_string(),
            kind: ChangeKind::Update {
                fields: vec!["name".to_string()],
            },
            state: None,
        };
        assert!(index.candidates(&other_type).is_empty());
    }
}
