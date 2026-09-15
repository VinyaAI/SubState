//! Shared process boot: dotenv, config, schema, CDS snapshot, poller, HTTP/WS.

use crate::hub::DeliveryHub;
use crate::poll;
use crate::ws;
use anyhow::{Context, Result};
use engine::Engine;
use postgres_source::Config;
use schema::SyncSchema;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;

pub struct Runtime {
    pub hub: Arc<DeliveryHub>,
    pub bind_addr: String,
    _poller: JoinHandle<()>,
    _server: JoinHandle<()>,
}

/// Load `.env` from `DOTENV_PATH`, then cwd `./.env`, then `components/cli/.env`.
pub fn load_dotenv() {
    if let Ok(path) = std::env::var("DOTENV_PATH") {
        let _ = dotenvy::from_path(Path::new(&path));
        return;
    }
    if dotenvy::dotenv().is_ok() {
        return;
    }
    let manifest_env = Path::new(env!("CARGO_MANIFEST_DIR")).join(".env");
    let _ = dotenvy::from_path(&manifest_env);
}

pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}

/// Connect, snapshot, spawn poller + Axum. Caller decides serve-forever vs shell.
pub async fn start() -> Result<Runtime> {
    let config = Config::from_env()?;
    let schema_path = resolve_schema_path(&config.schema_path);
    let sync_schema = Arc::new(
        SyncSchema::load_path(&schema_path)
            .with_context(|| format!("SCHEMA_PATH={}", schema_path.display()))?,
    );

    tracing::info!(
        schema = %config.schema,
        schema_path = %schema_path.display(),
        entities = sync_schema.entities.len(),
        poll_ms = config.poll_ms,
        bind_addr = %config.bind_addr,
        "starting SubState"
    );

    let pool = postgres_source::connect(&config.database_url).await?;
    let cds = postgres_source::snapshot(&pool, &config, &sync_schema).await?;
    let engine = Arc::new(RwLock::new(Engine::new(cds, (*sync_schema).clone())));
    let hub = Arc::new(DeliveryHub::new(Arc::clone(&engine)));

    let poller = poll::spawn(
        pool,
        config.schema.clone(),
        config.poll_ms,
        Arc::clone(&sync_schema),
        Arc::clone(&hub),
    );

    let server_hub = Arc::clone(&hub);
    let bind_addr = config.bind_addr.clone();
    let server = tokio::spawn(async move {
        if let Err(err) = ws::serve(&bind_addr, server_hub).await {
            tracing::error!(error = %err, "sync API exited");
        }
    });

    Ok(Runtime {
        hub,
        bind_addr: config.bind_addr,
        _poller: poller,
        _server: server,
    })
}

fn resolve_schema_path(configured: &Path) -> PathBuf {
    if configured.is_absolute() || configured.exists() {
        return configured.to_path_buf();
    }
    // Fall back to path relative to the CLI crate (useful for `cargo run` from anywhere).
    let via_manifest = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(configured);
    if via_manifest.exists() {
        return via_manifest;
    }
    configured.to_path_buf()
}
