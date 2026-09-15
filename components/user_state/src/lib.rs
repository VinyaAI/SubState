//! User State: what a subscription currently has.
//!
//! Materialized set of matching entities for one subscription. CDS changes are
//! turned into ADD / UPDATE / REMOVE membership transitions.

use cds::{Change, ChangeKind, EntityId, EntityState};
use subscription_index::{Subscription, SubscriptionIndex};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionKind {
    Add,
    Update,
    Remove,
}

#[derive(Debug, Clone)]
pub struct Transition {
    pub subscription_id: String,
    pub kind: TransitionKind,
    pub entity_id: EntityId,
    pub state: Option<EntityState>,
    /// On UPDATE, which CDS fields changed (for delta patches).
    pub changed_fields: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct UserState {
    pub subscription_id: String,
    pub entities: HashMap<EntityId, EntityState>,
}

impl UserState {
    pub fn new(subscription_id: impl Into<String>) -> Self {
        Self {
            subscription_id: subscription_id.into(),
            entities: HashMap::new(),
        }
    }

    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }

    /// Apply one CDS change against this subscription's filter and membership.
    pub fn apply_change(
        &mut self,
        sub: &Subscription,
        change: &Change,
    ) -> Option<Transition> {
        let entity_id = EntityId {
            entity_type: change.entity_type.clone(),
            id: change.id.clone(),
        };

        let was_in = self.entities.contains_key(&entity_id);
        let changed_fields = match &change.kind {
            ChangeKind::Update { fields } => Some(fields.clone()),
            _ => None,
        };

        match &change.kind {
            ChangeKind::Delete => {
                if !was_in {
                    return None;
                }
                self.entities.remove(&entity_id);
                Some(Transition {
                    subscription_id: self.subscription_id.clone(),
                    kind: TransitionKind::Remove,
                    entity_id,
                    state: None,
                    changed_fields: None,
                })
            }
            ChangeKind::Insert | ChangeKind::Update { .. } => {
                let Some(state) = change.state.as_ref() else {
                    return None;
                };
                let now_in = SubscriptionIndex::matches(sub, state);

                match (was_in, now_in) {
                    (false, true) => {
                        self.entities.insert(entity_id.clone(), state.clone());
                        Some(Transition {
                            subscription_id: self.subscription_id.clone(),
                            kind: TransitionKind::Add,
                            entity_id,
                            state: Some(state.clone()),
                            changed_fields: None,
                        })
                    }
                    (true, true) => {
                        self.entities.insert(entity_id.clone(), state.clone());
                        Some(Transition {
                            subscription_id: self.subscription_id.clone(),
                            kind: TransitionKind::Update,
                            entity_id,
                            state: Some(state.clone()),
                            changed_fields,
                        })
                    }
                    (true, false) => {
                        self.entities.remove(&entity_id);
                        Some(Transition {
                            subscription_id: self.subscription_id.clone(),
                            kind: TransitionKind::Remove,
                            entity_id,
                            state: None,
                            changed_fields: None,
                        })
                    }
                    (false, false) => None,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sub_available() -> Subscription {
        Subscription {
            id: "sub_1".to_string(),
            entity_type: "drivers".to_string(),
            where_eq: json!({"status": "available"}).as_object().cloned().unwrap(),
        }
    }

    fn entity(status: &str) -> EntityState {
        EntityState::from_fields(
            json!({"id": 1, "status": status})
                .as_object()
                .cloned()
                .unwrap(),
        )
    }

    #[test]
    fn membership_transitions() {
        let sub = sub_available();
        let mut us = UserState::new("sub_1");

        let add = Change {
            entity_type: "drivers".to_string(),
            id: "1".to_string(),
            kind: ChangeKind::Insert,
            state: Some(entity("available")),
        };
        let t = us.apply_change(&sub, &add).expect("add");
        assert_eq!(t.kind, TransitionKind::Add);
        assert_eq!(us.entity_count(), 1);

        let update = Change {
            entity_type: "drivers".to_string(),
            id: "1".to_string(),
            kind: ChangeKind::Update {
                fields: vec!["name".to_string()],
            },
            state: Some(EntityState::from_fields(
                json!({"id": 1, "status": "available", "name": "Alice"})
                    .as_object()
                    .cloned()
                    .unwrap(),
            )),
        };
        let t = us.apply_change(&sub, &update).expect("update");
        assert_eq!(t.kind, TransitionKind::Update);
        assert_eq!(t.changed_fields.as_deref(), Some(&["name".to_string()][..]));

        let leave = Change {
            entity_type: "drivers".to_string(),
            id: "1".to_string(),
            kind: ChangeKind::Update {
                fields: vec!["status".to_string()],
            },
            state: Some(entity("busy")),
        };
        let t = us.apply_change(&sub, &leave).expect("remove");
        assert_eq!(t.kind, TransitionKind::Remove);
        assert_eq!(us.entity_count(), 0);

        let _ = us.apply_change(&sub, &add);
        let delete = Change {
            entity_type: "drivers".to_string(),
            id: "1".to_string(),
            kind: ChangeKind::Delete,
            state: Some(entity("available")),
        };
        let t = us.apply_change(&sub, &delete).expect("delete remove");
        assert_eq!(t.kind, TransitionKind::Remove);
    }
}
