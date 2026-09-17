//! Ordered subscription deltas and per-subscription history.
//!
//! Sequence numbers are assigned only when a delta is emitted. History retains
//! the last 500 deltas so clients can `resume_after` a recent seq.

use cds::EntityState;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::VecDeque;
use user_state::{Transition, TransitionKind};

pub const HISTORY_CAP: usize = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaOp {
    Add,
    Update,
    Remove,
}

/// One sequenced change for a subscription (wire-friendly).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delta {
    pub subscription: String,
    pub seq: u64,
    pub op: DeltaOp,
    pub entity: String,
    pub id: String,
    /// Full fields on add; patch on update; none on remove.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Map<String, Value>>,
}

/// One entity row inside a snapshot message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotEntity {
    pub id: String,
    pub fields: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetNeeded;

/// Ring buffer of the last [`HISTORY_CAP`] deltas for one subscription.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaHistory {
    next_seq: u64,
    entries: VecDeque<Delta>,
    last_ack: u64,
}

impl Default for DeltaHistory {
    fn default() -> Self {
        Self::new()
    }
}

impl DeltaHistory {
    pub fn new() -> Self {
        Self {
            next_seq: 1,
            entries: VecDeque::new(),
            last_ack: 0,
        }
    }

    pub fn last_seq(&self) -> u64 {
        self.next_seq.saturating_sub(1)
    }

    pub fn last_ack(&self) -> u64 {
        self.last_ack
    }

    pub fn ack(&mut self, seq: u64) {
        if seq > self.last_ack {
            self.last_ack = seq;
        }
    }

    /// Assign the next sequence number, push into history, return the delta.
    pub fn push(&mut self, mut delta: Delta) -> Delta {
        delta.seq = self.next_seq;
        self.next_seq += 1;
        self.entries.push_back(delta.clone());
        while self.entries.len() > HISTORY_CAP {
            self.entries.pop_front();
        }
        delta
    }

    /// Deltas with `seq > resume_after`, or [`ResetNeeded`] if history is too old.
    pub fn since(&self, resume_after: u64) -> Result<Vec<Delta>, ResetNeeded> {
        if let Some(oldest) = self.entries.front().map(|d| d.seq) {
            // Client needs seq resume_after+1 onward; if we dropped it, reset.
            if resume_after + 1 < oldest {
                return Err(ResetNeeded);
            }
        } else if resume_after > 0 && self.next_seq > 1 {
            // History empty but we previously emitted deltas that are gone.
            return Err(ResetNeeded);
        }

        Ok(self
            .entries
            .iter()
            .filter(|d| d.seq > resume_after)
            .cloned()
            .collect())
    }
}

/// Build an unsequenced delta from a User State transition (seq filled by history).
pub fn from_transition(transition: &Transition) -> Delta {
    let op = match transition.kind {
        TransitionKind::Add => DeltaOp::Add,
        TransitionKind::Update => DeltaOp::Update,
        TransitionKind::Remove => DeltaOp::Remove,
    };

    let fields = match transition.kind {
        TransitionKind::Remove => None,
        TransitionKind::Add => transition.state.as_ref().map(|s| s.fields.clone()),
        TransitionKind::Update => {
            let state = transition.state.as_ref();
            match (&transition.changed_fields, state) {
                (Some(keys), Some(state)) if !keys.is_empty() => {
                    let mut patch = Map::new();
                    for key in keys {
                        if let Some(value) = state.fields.get(key) {
                            patch.insert(key.clone(), value.clone());
                        }
                    }
                    Some(patch)
                }
                (_, Some(state)) => Some(state.fields.clone()),
                _ => None,
            }
        }
    };

    Delta {
        subscription: transition.subscription_id.clone(),
        seq: 0,
        op,
        entity: transition.entity_id.entity_type.clone(),
        id: transition.entity_id.id.clone(),
        fields,
    }
}

/// Build snapshot entities from a User State map (sorted by id).
pub fn snapshot_entities(
    entities: &std::collections::HashMap<cds::EntityId, EntityState>,
) -> Vec<SnapshotEntity> {
    let mut items: Vec<_> = entities
        .iter()
        .map(|(id, state)| SnapshotEntity {
            id: id.id.clone(),
            fields: state.fields.clone(),
        })
        .collect();
    items.sort_by(|a, b| a.id.cmp(&b.id));
    items
}

#[cfg(test)]
mod tests {
    use super::*;
    use cds::{EntityId, EntityState};
    use serde_json::json;

    fn transition_update() -> Transition {
        Transition {
            subscription_id: "sub_1".to_string(),
            kind: TransitionKind::Update,
            entity_id: EntityId {
                entity_type: "drivers".to_string(),
                id: "728".to_string(),
            },
            state: Some(EntityState::from_fields(
                json!({"id": 728, "name": "Alice", "status": "busy"})
                    .as_object()
                    .cloned()
                    .unwrap(),
            )),
            changed_fields: Some(vec!["status".to_string()]),
        }
    }

    #[test]
    fn from_transition_maps_ops_and_patches() {
        let add = Transition {
            subscription_id: "sub_1".to_string(),
            kind: TransitionKind::Add,
            entity_id: EntityId {
                entity_type: "drivers".to_string(),
                id: "1".to_string(),
            },
            state: Some(EntityState::from_fields(
                json!({"id": 1, "status": "available"})
                    .as_object()
                    .cloned()
                    .unwrap(),
            )),
            changed_fields: None,
        };
        let d = from_transition(&add);
        assert_eq!(d.op, DeltaOp::Add);
        assert!(d.fields.as_ref().unwrap().contains_key("status"));

        let d = from_transition(&transition_update());
        assert_eq!(d.op, DeltaOp::Update);
        let fields = d.fields.unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields.get("status"), Some(&json!("busy")));

        let remove = Transition {
            subscription_id: "sub_1".to_string(),
            kind: TransitionKind::Remove,
            entity_id: EntityId {
                entity_type: "drivers".to_string(),
                id: "1".to_string(),
            },
            state: None,
            changed_fields: None,
        };
        let d = from_transition(&remove);
        assert_eq!(d.op, DeltaOp::Remove);
        assert!(d.fields.is_none());
    }

    #[test]
    fn history_assigns_seq_caps_and_replays() {
        let mut history = DeltaHistory::new();
        for i in 0..3 {
            let mut t = transition_update();
            t.entity_id.id = i.to_string();
            history.push(from_transition(&t));
        }
        assert_eq!(history.last_seq(), 3);
        let replay = history.since(1).unwrap();
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].seq, 2);
        assert_eq!(replay[1].seq, 3);
    }

    #[test]
    fn history_reset_when_too_old() {
        let mut history = DeltaHistory::new();
        for i in 0..(HISTORY_CAP + 10) {
            let mut t = transition_update();
            t.entity_id.id = i.to_string();
            history.push(from_transition(&t));
        }
        // Oldest retained seq is 11; resume_after=0 cannot cover seq 1..10.
        assert!(history.since(0).is_err());
        let oldest = history.entries.front().unwrap().seq;
        assert!(history.since(oldest - 1).is_ok());
    }

    #[test]
    fn ack_advances_cursor() {
        let mut history = DeltaHistory::new();
        history.push(from_transition(&transition_update()));
        history.ack(1);
        assert_eq!(history.last_ack(), 1);
        history.ack(0);
        assert_eq!(history.last_ack(), 1);
    }
}
