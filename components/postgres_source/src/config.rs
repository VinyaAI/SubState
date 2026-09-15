//! Process configuration loaded from environment variables / `.env`.
//!
//! Required:
//! - `DATABASE_URL` — any Postgres connection string
//! - `SCHEMA_PATH` — path to handwritten sync schema YAML
//!
//! Optional:
//! - `CDS_SCHEMA` — Postgres schema to introspect (default: `public`)
//! - `CDS_TABLES` — unused under schema mode except for warnings
//! - `CDS_POLL_MS` — poll interval in milliseconds (default: `2000`)
//! - `BIND_ADDR` — HTTP/WebSocket listen address (default: `127.0.0.1:8080`)

use anyhow::{Context, Result};
use std::env;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub schema: String,
    pub tables: Option<Vec<String>>,
    pub poll_ms: u64,
    pub bind_addr: String,
    /// Path to handwritten sync schema YAML.
    pub schema_path: PathBuf,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let database_url = env::var("DATABASE_URL").context("DATABASE_URL is required")?;
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

        Ok(Self {
            database_url,
            schema,
            tables,
            poll_ms,
            bind_addr,
            schema_path,
        })
    }
}

fn parse_tables(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|table| !table.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::parse_tables;

    #[test]
    fn parse_tables_splits_and_trims() {
        assert_eq!(
            parse_tables(" drivers, jobs , "),
            vec!["drivers".to_string(), "jobs".to_string()]
        );
    }

    #[test]
    fn parse_tables_empty_is_empty() {
        assert!(parse_tables("").is_empty());
        assert!(parse_tables("  , , ").is_empty());
    }
}
