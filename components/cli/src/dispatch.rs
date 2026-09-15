//! Inbox dispatcher: [`SourceEvent`] → engine → sequenced deltas.

use crate::hub::DeliveryHub;
use source::SourceEvent;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::error;

pub fn spawn(hub: Arc<DeliveryHub>, mut rx: mpsc::Receiver<SourceEvent>) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            match event {
                SourceEvent::Upsert(update) => {
                    if let Err(err) = hub.apply_and_publish(update).await {
                        error!(error = %err, "source upsert rejected");
                    }
                }
                SourceEvent::Delete { entity_type, id } => {
                    hub.remove_and_publish(&entity_type, &id).await;
                }
                SourceEvent::Reconcile {
                    entity_type,
                    present,
                } => {
                    hub.reconcile_and_publish(&entity_type, &present).await;
                }
            }
        }
    })
}
