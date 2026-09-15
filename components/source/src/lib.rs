//! Shared inbox for source adapters.
//!
//! Adapters never talk to the CDS directly. They emit [`SourceEvent`]s on a
//! channel; the CLI dispatcher applies them through the engine.

use cds::SourceUpdate;
use std::collections::HashSet;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// One change from any source adapter (Postgres, Kafka, …).
#[derive(Debug, Clone)]
pub enum SourceEvent {
    Upsert(SourceUpdate),
    Delete { entity_type: String, id: String },
    /// Poll follow: drop CDS identities of this type that are not in `present`.
    Reconcile {
        entity_type: String,
        present: HashSet<String>,
    },
}

/// Background follow loop that streams [`SourceEvent`]s into the inbox.
pub trait Follow: Send + 'static {
    fn spawn(self: Box<Self>, tx: mpsc::Sender<SourceEvent>) -> JoinHandle<()>;
}
