//! Shared process boot: dotenv, config, schema, CDS snapshot, source follows, HTTP/WS.

use crate::config::{PostgresFollowMode, RuntimeConfig};
use crate::dispatch;
use crate::hub::DeliveryHub;
use crate::ws;
use anyhow::{bail, Context, Result};
use engine::Engine;
use kafka_source::KafkaFollow;
use postgres_source::cdc::{
    ensure_publication, prepare_slot, wal_is_logical, PostgresCdcFollow,
};
use postgres_source::poll::PostgresPollFollow;
use schema::{SyncSchema, SOURCE_KAFKA, SOURCE_POSTGRES};
use source::Follow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;

pub struct Runtime {
    pub hub: Arc<DeliveryHub>,
    pub bind_addr: String,
    _tasks: Vec<JoinHandle<()>>,
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

/// Connect, snapshot, spawn follows + Axum. Caller decides serve-forever vs shell.
pub async fn start() -> Result<Runtime> {
    let config = RuntimeConfig::from_env()?;
    let schema_path = resolve_schema_path(&config.schema_path);
    let sync_schema = Arc::new(
        SyncSchema::load_path(&schema_path)
            .with_context(|| format!("SCHEMA_PATH={}", schema_path.display()))?,
    );

    let wants_postgres = sync_schema.has_source_type(SOURCE_POSTGRES);
    let wants_kafka = sync_schema.has_source_type(SOURCE_KAFKA);

    tracing::info!(
        schema = %config.schema,
        schema_path = %schema_path.display(),
        entities = sync_schema.entities.len(),
        postgres = wants_postgres,
        kafka = wants_kafka,
        postgres_follow = ?config.postgres_follow,
        bind_addr = %config.bind_addr,
        "starting SubState"
    );

    if wants_postgres && config.database_url.is_none() {
        bail!("schema has a postgres source but DATABASE_URL is not set");
    }
    if wants_kafka && config.kafka_brokers.is_none() {
        bail!("schema has a kafka source but KAFKA_BROKERS is not set");
    }

    let (cds, pg_pool) = if wants_postgres {
        let url = config.database_url.as_deref().unwrap();
        let pool = postgres_source::connect(url).await?;
        let snap = config.postgres_snapshot_config();
        let cds = postgres_source::snapshot(&pool, &snap, &sync_schema).await?;
        (cds, Some(pool))
    } else {
        (
            postgres_source::catalog_only(config.schema.clone(), &sync_schema),
            None,
        )
    };

    let engine = Arc::new(RwLock::new(Engine::new(cds, (*sync_schema).clone())));
    let hub = Arc::new(DeliveryHub::new(Arc::clone(&engine)));

    let snapshot_path = crate::persist::default_snapshot_path();
    match hub.restore_snapshot(&snapshot_path).await {
        Ok(true) => tracing::info!(path = %snapshot_path.display(), "loaded SubState snapshot"),
        Ok(false) => {}
        Err(err) => tracing::warn!(error = %err, path = %snapshot_path.display(), "failed to load snapshot"),
    }

    let (tx, rx) = mpsc::channel(1024);
    let mut tasks = vec![dispatch::spawn(Arc::clone(&hub), rx)];

    if let Some(pool) = pg_pool {
        tasks.push(start_postgres_follow(&config, &sync_schema, pool, tx.clone()).await?);
    }

    if wants_kafka {
        let follow = KafkaFollow {
            brokers: config.kafka_brokers.clone().unwrap(),
            sync_schema: Arc::clone(&sync_schema),
        };
        tasks.push(Box::new(follow).spawn(tx));
    }

    if config.api_key.is_none() {
        tracing::warn!(
            "SUBSTATE_API_KEY is unset; /v1/* is open (local-dev only). Set a shared secret before exposing beyond localhost."
        );
    }

    let coalesce_hub = Arc::clone(&hub);
    tasks.push(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(25));
        loop {
            ticker.tick().await;
            coalesce_hub.flush_coalesced().await;
        }
    }));

    let persist_hub = Arc::clone(&hub);
    let persist_path = snapshot_path.clone();
    tasks.push(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            ticker.tick().await;
            if let Err(err) = persist_hub.save_snapshot(&persist_path).await {
                tracing::warn!(error = %err, "failed to save SubState snapshot");
            }
        }
    }));

    let server_hub = Arc::clone(&hub);
    let bind_addr = config.bind_addr.clone();
    let api_key = config.api_key.clone();
    tasks.push(tokio::spawn(async move {
        if let Err(err) = ws::serve(&bind_addr, server_hub, api_key).await {
            tracing::error!(error = %err, "sync API exited");
        }
    }));

    Ok(Runtime {
        hub,
        bind_addr: config.bind_addr,
        _tasks: tasks,
    })
}

async fn start_postgres_follow(
    config: &RuntimeConfig,
    sync_schema: &Arc<SyncSchema>,
    pool: postgres_source::PgPool,
    tx: mpsc::Sender<source::SourceEvent>,
) -> Result<JoinHandle<()>> {
    let poll = || {
        PostgresPollFollow {
            pool: pool.clone(),
            pg_schema: config.schema.clone(),
            poll_ms: config.poll_ms,
            sync_schema: Arc::clone(sync_schema),
        }
    };

    match config.postgres_follow {
        PostgresFollowMode::Poll => Ok(Box::new(poll()).spawn(tx)),
        PostgresFollowMode::Cdc | PostgresFollowMode::Auto => {
            let try_cdc = async {
                if !wal_is_logical(&pool).await? {
                    bail!("wal_level is not logical");
                }
                let tables = sync_schema.postgres_tables();
                ensure_publication(&pool, &config.schema, &tables).await?;
                prepare_slot(&pool).await?;
                Ok::<(), anyhow::Error>(())
            };

            match try_cdc.await {
                Ok(()) => {
                    let follow = PostgresCdcFollow {
                        pool: pool.clone(),
                        sync_schema: Arc::clone(sync_schema),
                        interval_ms: 200,
                    };
                    Ok(Box::new(follow).spawn(tx))
                }
                Err(err) if config.postgres_follow == PostgresFollowMode::Auto => {
                    tracing::warn!(error = %err, "postgres CDC unavailable; falling back to poll");
                    Ok(Box::new(poll()).spawn(tx))
                }
                Err(err) => Err(err).context("POSTGRES_FOLLOW=cdc"),
            }
        }
    }
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
