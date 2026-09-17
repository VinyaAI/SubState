//! Subscription Index: who cares about which CDS fields?
//!
//! Filters support:
//! - equality: `{ "status": "available" }` (AND across keys)
//! - comparisons: `{ "speed": { "gt": 5 } }` / `gte` / `lt` / `lte`
//! - OR: `{ "$or": [ { "status": "available" }, { "status": "busy" } ] }`
//! - nested AND via `$and`

use cds::{Change, EntityState};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};

pub type SubId = String;

#[derive(Debug, Clone)]
pub struct Subscription {
    pub id: SubId,
    pub entity_type: String,
    /// Filter document (equality / comparisons / $or / $and).
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
        self.subscribe_with_id(id, entity_type, where_eq)
    }

    /// Restore a subscription with a known id (for disk snapshots).
    pub fn subscribe_with_id(
        &mut self,
        id: impl Into<String>,
        entity_type: impl Into<String>,
        where_eq: Map<String, Value>,
    ) -> Subscription {
        let id = id.into();
        if let Some(num) = id.strip_prefix("sub_").and_then(|s| s.parse::<u64>().ok()) {
            if num > self.next_id {
                self.next_id = num;
            }
        }
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

        for field in referenced_fields(&where_eq) {
            self.by_field
                .entry((entity_type.clone(), field))
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
        for field in referenced_fields(&sub.where_eq) {
            if let Some(list) = self.by_field.get_mut(&(sub.entity_type.clone(), field)) {
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

    pub fn candidates(&self, change: &Change) -> Vec<SubId> {
        let mut ids: HashSet<SubId> = HashSet::new();

        if let Some(list) = self.by_type.get(&change.entity_type) {
            ids.extend(list.iter().cloned());
        }

        let mut out: Vec<_> = ids.into_iter().collect();
        out.sort();
        out
    }

    /// Evaluate the subscription filter against entity state.
    pub fn matches(sub: &Subscription, state: &EntityState) -> bool {
        matches_doc(&sub.where_eq, state)
    }
}

fn referenced_fields(doc: &Map<String, Value>) -> Vec<String> {
    let mut out = Vec::new();
    collect_fields(doc, &mut out);
    out.sort();
    out.dedup();
    out
}

fn collect_fields(doc: &Map<String, Value>, out: &mut Vec<String>) {
    for (key, value) in doc {
        if key == "$or" || key == "$and" {
            if let Some(arr) = value.as_array() {
                for item in arr {
                    if let Some(obj) = item.as_object() {
                        collect_fields(obj, out);
                    }
                }
            }
            continue;
        }
        out.push(key.clone());
    }
}

fn matches_doc(doc: &Map<String, Value>, state: &EntityState) -> bool {
    if doc.is_empty() {
        return true;
    }
    for (key, expected) in doc {
        if key == "$or" {
            let Some(arr) = expected.as_array() else {
                return false;
            };
            if arr.is_empty() {
                return false;
            }
            let any = arr.iter().any(|item| {
                item.as_object()
                    .map(|obj| matches_doc(obj, state))
                    .unwrap_or(false)
            });
            if !any {
                return false;
            }
            continue;
        }
        if key == "$and" {
            let Some(arr) = expected.as_array() else {
                return false;
            };
            if !arr.iter().all(|item| {
                item.as_object()
                    .map(|obj| matches_doc(obj, state))
                    .unwrap_or(false)
            }) {
                return false;
            }
            continue;
        }
        let Some(actual) = state.fields.get(key) else {
            return false;
        };
        if !match_predicate(actual, expected) {
            return false;
        }
    }
    true
}

fn match_predicate(actual: &Value, expected: &Value) -> bool {
    match expected {
        Value::Object(ops) if is_operator_object(ops) => {
            for (op, rhs) in ops {
                match op.as_str() {
                    "eq" => {
                        if actual != rhs {
                            return false;
                        }
                    }
                    "gt" => {
                        if !compare(actual, rhs).map(|o| o.is_gt()).unwrap_or(false) {
                            return false;
                        }
                    }
                    "gte" => {
                        if !compare(actual, rhs).map(|o| o.is_ge()).unwrap_or(false) {
                            return false;
                        }
                    }
                    "lt" => {
                        if !compare(actual, rhs).map(|o| o.is_lt()).unwrap_or(false) {
                            return false;
                        }
                    }
                    "lte" => {
                        if !compare(actual, rhs).map(|o| o.is_le()).unwrap_or(false) {
                            return false;
                        }
                    }
                    "ne" => {
                        if actual == rhs {
                            return false;
                        }
                    }
                    _ => return false,
                }
            }
            true
        }
        other => actual == other,
    }
}

fn is_operator_object(ops: &Map<String, Value>) -> bool {
    !ops.is_empty()
        && ops.keys().all(|k| {
            matches!(
                k.as_str(),
                "eq" | "gt" | "gte" | "lt" | "lte" | "ne"
            )
        })
}

fn compare(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Number(a), Value::Number(b)) => {
            let af = a.as_f64()?;
            let bf = b.as_f64()?;
            af.partial_cmp(&bf)
        }
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (Value::String(a), Value::Number(b)) => {
            let af: f64 = a.parse().ok()?;
            let bf = b.as_f64()?;
            af.partial_cmp(&bf)
        }
        (Value::Number(a), Value::String(b)) => {
            let af = a.as_f64()?;
            let bf: f64 = b.parse().ok()?;
            af.partial_cmp(&bf)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cds::{ChangeKind, EntityState};
    use serde_json::json;

    fn state(status: &str, region: &str) -> EntityState {
        EntityState::from_fields(
            json!({"status": status, "region": region, "speed": 10})
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
    fn matches_range_and_or() {
        let mut index = SubscriptionIndex::new();
        let sub = index.subscribe(
            "drivers",
            json!({
                "speed": { "gte": 5 },
                "$or": [
                    { "status": "available" },
                    { "status": "busy" }
                ]
            })
            .as_object()
            .cloned()
            .unwrap(),
        );
        assert!(SubscriptionIndex::matches(&sub, &state("available", "x")));
        assert!(SubscriptionIndex::matches(&sub, &state("busy", "x")));
        assert!(!SubscriptionIndex::matches(
            &sub,
            &EntityState::from_fields(
                json!({"status": "offline", "speed": 10})
                    .as_object()
                    .cloned()
                    .unwrap()
            )
        ));
        assert!(!SubscriptionIndex::matches(
            &sub,
            &EntityState::from_fields(
                json!({"status": "available", "speed": 1})
                    .as_object()
                    .cloned()
                    .unwrap()
            )
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
