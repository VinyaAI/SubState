//! Process configuration loaded from environment variables / `.env`.

use anyhow::{bail, Context, Result};
use postgres_source::config::parse_tables;
use postgres_source::Config as PgConfig;
use std::env;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresFollowMode {
    Auto,
    Cdc,
    Poll,
}

impl PostgresFollowMode {
    fn from_env() -> Result<Self> {
        let raw = env::var("POSTGRES_FOLLOW").unwrap_or_else(|_| "auto".to_string());
        match raw.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "cdc" => Ok(Self::Cdc),
            "poll" => Ok(Self::Poll),
            other => bail!("POSTGRES_FOLLOW must be auto, cdc, or poll (got {other})"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub database_url: Option<String>,
    pub schema: String,
    pub tables: Option<Vec<String>>,
    pub poll_ms: u64,
    pub bind_addr: String,
    pub schema_path: PathBuf,
    pub kafka_brokers: Option<Vec<String>>,
    pub postgres_follow: PostgresFollowMode,
    /// When set, `/v1/*` requires this shared secret. `/health` stays open.
    pub api_key: Option<String>,
}

impl RuntimeConfig {
    pub fn from_env() -> Result<Self> {
        let schema = env::var("CDS_SCHEMA").unwrap_or_else(|_| "public".to_string());
        let tables = env::var("CDS_TABLES").ok().and_then(|value| {
            let tables = parse_tables(&value);
            if tables.is_empty() {
                None
            } else {
                Some(tables)
            }
        });
        let poll_ms = env::var("CDS_POLL_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(2000);
        let bind_addr = env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
        let schema_path = env::var("SCHEMA_PATH")
            .map(PathBuf::from)
            .context("SCHEMA_PATH is required (path to sync schema YAML)")?;
        let database_url = env::var("DATABASE_URL").ok().filter(|s| !s.is_empty());
        let kafka_brokers = env::var("KAFKA_BROKERS").ok().and_then(|value| {
            let brokers: Vec<String> = value
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
                .collect();
            if brokers.is_empty() {
                None
            } else {
                Some(brokers)
            }
        });
        let api_key = env::var("SUBSTATE_API_KEY")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        Ok(Self {
            database_url,
            schema,
            tables,
            poll_ms,
            bind_addr,
            schema_path,
            kafka_brokers,
            postgres_follow: PostgresFollowMode::from_env()?,
            api_key,
        })
    }

    pub fn postgres_snapshot_config(&self) -> PgConfig {
        PgConfig::new(self.schema.clone(), self.tables.clone())
    }
}
