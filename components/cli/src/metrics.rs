//! Lightweight Prometheus text metrics for the sidecar.

use std::sync::atomic::{AtomicU64, Ordering};

static WS_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
static INGEST_ACCEPTED: AtomicU64 = AtomicU64::new(0);
static INGEST_REJECTED: AtomicU64 = AtomicU64::new(0);
static DELTAS_PUBLISHED: AtomicU64 = AtomicU64::new(0);
static COALESCE_FLUSHES: AtomicU64 = AtomicU64::new(0);
static SNAPSHOT_SAVES: AtomicU64 = AtomicU64::new(0);
static WS_BACKPRESSURE_RESETS: AtomicU64 = AtomicU64::new(0);

pub fn ws_connect() {
    WS_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
}

pub fn ws_disconnect() {
    WS_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
}

pub fn ingest_accepted() {
    INGEST_ACCEPTED.fetch_add(1, Ordering::Relaxed);
}

pub fn ingest_rejected() {
    INGEST_REJECTED.fetch_add(1, Ordering::Relaxed);
}

pub fn deltas_published(n: u64) {
    DELTAS_PUBLISHED.fetch_add(n, Ordering::Relaxed);
}

pub fn coalesce_flush() {
    COALESCE_FLUSHES.fetch_add(1, Ordering::Relaxed);
}

pub fn snapshot_saved() {
    SNAPSHOT_SAVES.fetch_add(1, Ordering::Relaxed);
}

pub fn ws_backpressure_reset() {
    WS_BACKPRESSURE_RESETS.fetch_add(1, Ordering::Relaxed);
}

pub fn render_prometheus() -> String {
    format!(
        "# HELP substate_ws_connections Open WebSocket connections.\n\
         # TYPE substate_ws_connections gauge\n\
         substate_ws_connections {}\n\
         # HELP substate_ingest_accepted_total Accepted ingest updates.\n\
         # TYPE substate_ingest_accepted_total counter\n\
         substate_ingest_accepted_total {}\n\
         # HELP substate_ingest_rejected_total Rejected ingest updates.\n\
         # TYPE substate_ingest_rejected_total counter\n\
         substate_ingest_rejected_total {}\n\
         # HELP substate_deltas_published_total Sequenced deltas published.\n\
         # TYPE substate_deltas_published_total counter\n\
         substate_deltas_published_total {}\n\
         # HELP substate_coalesce_flushes_total Latest-value coalesce flush cycles with work.\n\
         # TYPE substate_coalesce_flushes_total counter\n\
         substate_coalesce_flushes_total {}\n\
         # HELP substate_snapshot_saves_total Disk snapshot writes.\n\
         # TYPE substate_snapshot_saves_total counter\n\
         substate_snapshot_saves_total {}\n\
         # HELP substate_ws_backpressure_resets_total Sessions reset due to slow clients.\n\
         # TYPE substate_ws_backpressure_resets_total counter\n\
         substate_ws_backpressure_resets_total {}\n",
        WS_CONNECTIONS.load(Ordering::Relaxed),
        INGEST_ACCEPTED.load(Ordering::Relaxed),
        INGEST_REJECTED.load(Ordering::Relaxed),
        DELTAS_PUBLISHED.load(Ordering::Relaxed),
        COALESCE_FLUSHES.load(Ordering::Relaxed),
        SNAPSHOT_SAVES.load(Ordering::Relaxed),
        WS_BACKPRESSURE_RESETS.load(Ordering::Relaxed),
    )
}
