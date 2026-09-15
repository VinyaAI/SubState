//! Delivery hub: per-subscription delta history + broadcast to shell / WebSocket.

use delta::{from_transition, snapshot_entities, Delta, DeltaHistory, ResetNeeded, SnapshotEntity};
use engine::Engine;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use user_state::Transition;

/// Events observed by the shell and WebSocket layer.
#[derive(Debug, Clone)]
pub enum HubEvent {
    /// Sequenced delta after poller fan-out.
    Delta(Delta),
}

pub struct DeliveryHub {
    engine: Arc<RwLock<Engine>>,
    histories: RwLock<HashMap<String, SubDelivery>>,
    tx: broadcast::Sender<HubEvent>,
}

struct SubDelivery {
    history: DeltaHistory,
}

impl DeliveryHub {
    pub fn new(engine: Arc<RwLock<Engine>>) -> Self {
        let (tx, _) = broadcast::channel(256);
        Self {
            engine,
            histories: RwLock::new(HashMap::new()),
            tx,
        }
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<HubEvent> {
        self.tx.subscribe()
    }

    /// Process poller transitions: assign seqs, store history, broadcast deltas.
    pub async fn publish_transitions(&self, transitions: Vec<Transition>) {
        let mut histories = self.histories.write().await;
        for transition in &transitions {
            let delivery = histories
                .entry(transition.subscription_id.clone())
                .or_insert_with(|| SubDelivery {
                    history: DeltaHistory::new(),
                });
            let delta = delivery.history.push(from_transition(transition));
            let _ = self.tx.send(HubEvent::Delta(delta));
        }
    }

    /// Apply a source update through the engine and publish resulting deltas.
    pub async fn apply_and_publish(
        &self,
        update: cds::SourceUpdate,
    ) -> Result<Vec<String>, String> {
        let (changed_fields, transitions) = {
            let mut engine = self.engine.write().await;
            engine.apply_source_update(update)?
        };
        self.publish_transitions(transitions).await;
        Ok(changed_fields)
    }

    /// Create engine subscription + history; return id and snapshot entities.
    pub async fn subscribe(
        &self,
        entity_type: String,
        where_eq: Map<String, Value>,
    ) -> Result<(String, Vec<SnapshotEntity>), String> {
        let (sub_id, entities) = {
            let mut engine = self.engine.write().await;
            let (sub, user_state) = engine.subscribe(entity_type, where_eq)?;
            let entities = snapshot_entities(&user_state.entities);
            (sub.id, entities)
        };
        let mut histories = self.histories.write().await;
        histories.insert(
            sub_id.clone(),
            SubDelivery {
                history: DeltaHistory::new(),
            },
        );
        Ok((sub_id, entities))
    }

    pub async fn unsubscribe(&self, sub_id: &str) -> bool {
        let removed = {
            let mut engine = self.engine.write().await;
            engine.unsubscribe(sub_id)
        };
        self.histories.write().await.remove(sub_id);
        removed
    }

    pub async fn ack(&self, sub_id: &str, seq: u64) -> Result<(), String> {
        let mut histories = self.histories.write().await;
        let delivery = histories
            .get_mut(sub_id)
            .ok_or_else(|| format!("unknown subscription '{sub_id}'"))?;
        delivery.history.ack(seq);
        Ok(())
    }

    pub async fn resume_since(
        &self,
        sub_id: &str,
        resume_after: u64,
    ) -> Result<Result<Vec<Delta>, ResetNeeded>, String> {
        let histories = self.histories.read().await;
        let delivery = histories
            .get(sub_id)
            .ok_or_else(|| format!("unknown subscription '{sub_id}'"))?;
        Ok(delivery.history.since(resume_after))
    }

    pub async fn snapshot_for(&self, sub_id: &str) -> Result<Vec<SnapshotEntity>, String> {
        let engine = self.engine.read().await;
        let user_state = engine
            .user_state(sub_id)
            .ok_or_else(|| format!("unknown subscription '{sub_id}'"))?;
        Ok(snapshot_entities(&user_state.entities))
    }

    pub async fn has_subscription(&self, sub_id: &str) -> bool {
        self.histories.read().await.contains_key(sub_id)
    }

    pub fn engine(&self) -> Arc<RwLock<Engine>> {
        Arc::clone(&self.engine)
    }
}

/// Track which subscription ids a single WebSocket connection is attached to.
#[derive(Debug, Default)]
pub struct SessionSubs {
    ids: HashSet<String>,
}

impl SessionSubs {
    pub fn insert(&mut self, id: String) {
        self.ids.insert(id);
    }

    pub fn remove(&mut self, id: &str) {
        self.ids.remove(id);
    }

    pub fn contains(&self, id: &str) -> bool {
        self.ids.contains(id)
    }
}
