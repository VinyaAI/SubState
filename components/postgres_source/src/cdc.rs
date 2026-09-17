//! Logical decoding follow via `pgoutput` + `pg_logical_slot_get_binary_changes`.
//!
//! Uses a normal sqlx connection (no replication protocol). The slot still
//! reads the WAL; we poll it on a short interval.

use crate::pgoutput::{parse_lsn, parse_messages, tuple_to_object, PgOutput, Relation};
use crate::{project_postgres_rows, quote_ident};
use anyhow::{Context, Result};
use schema::SyncSchema;
use source::{Follow, SourceEvent};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

pub const SLOT_NAME: &str = "substate";
pub const PUBLICATION_NAME: &str = "substate";

pub struct PostgresCdcFollow {
    pub pool: PgPool,
    pub sync_schema: Arc<SyncSchema>,
    pub interval_ms: u64,
}

impl Follow for PostgresCdcFollow {
    fn spawn(self: Box<Self>, tx: mpsc::Sender<SourceEvent>) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!(slot = SLOT_NAME, "postgres CDC follow started");
            let interval = Duration::from_millis(self.interval_ms.max(50));
            let mut relations: HashMap<u32, Relation> = HashMap::new();
            loop {
                if let Err(err) =
                    drain_slot(&self.pool, &self.sync_schema, &mut relations, &tx).await
                {
                    error!(error = %err, "postgres CDC poll failed");
                }
                tokio::time::sleep(interval).await;
            }
        })
    }
}

/// True when the server can offer logical decoding.
pub async fn wal_is_logical(pool: &PgPool) -> Result<bool> {
    let row: (String,) = sqlx::query_as("SHOW wal_level")
        .fetch_one(pool)
        .await
        .context("SHOW wal_level")?;
    Ok(row.0 == "logical")
}

pub async fn ensure_publication(pool: &PgPool, pg_schema: &str, tables: &[String]) -> Result<()> {
    let existing: Option<(String,)> =
        sqlx::query_as("SELECT pubname FROM pg_publication WHERE pubname = $1")
            .bind(PUBLICATION_NAME)
            .fetch_optional(pool)
            .await
            .context("lookup publication")?;
    if existing.is_none() {
        sqlx::query("CREATE PUBLICATION substate")
            .execute(pool)
            .await
            .context("CREATE PUBLICATION")?;
    }

    for table in tables {
        let qualified = format!("{}.{}", quote_ident(pg_schema), quote_ident(table));
        let sql = format!(
            "ALTER PUBLICATION {} ADD TABLE {}",
            quote_ident(PUBLICATION_NAME),
            qualified
        );
        if let Err(err) = sqlx::query(sqlx::AssertSqlSafe(sql)).execute(pool).await {
            let message = err.to_string();
            if message.contains("already member") {
                continue;
            }
            return Err(err).with_context(|| format!("ALTER PUBLICATION add {qualified}"));
        }
    }
    info!(tables = tables.len(), "postgres publication ready");
    Ok(())
}

pub async fn prepare_slot(pool: &PgPool) -> Result<()> {
    let existing: Option<(String,)> = sqlx::query_as(
        "SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(SLOT_NAME)
    .fetch_optional(pool)
    .await
    .context("lookup replication slot")?;

    if existing.is_some() {
        info!(slot = SLOT_NAME, "reusing postgres replication slot");
        return Ok(());
    }

    sqlx::query("SELECT * FROM pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind(SLOT_NAME)
        .execute(pool)
        .await
        .context("pg_create_logical_replication_slot")?;
    info!(slot = SLOT_NAME, "created postgres replication slot");
    Ok(())
}

async fn drain_slot(
    pool: &PgPool,
    sync_schema: &SyncSchema,
    relations: &mut HashMap<u32, Relation>,
    tx: &mpsc::Sender<SourceEvent>,
) -> Result<()> {
    let rows: Vec<(String, Vec<u8>)> = sqlx::query_as(
        r#"
        SELECT lsn::text, data
        FROM pg_logical_slot_get_binary_changes($1, NULL, $2, 'proto_version', '1', 'publication_names', $3)
        "#,
    )
    .bind(SLOT_NAME)
    .bind(500i32)
    .bind(PUBLICATION_NAME)
    .fetch_all(pool)
    .await
    .context("pg_logical_slot_get_binary_changes")?;

    for (lsn_text, data) in rows {
        let version = match parse_lsn(&lsn_text) {
            Ok(lsn) => lsn,
            Err(err) => {
                warn!(lsn = %lsn_text, error = %err, "skip CDC row with bad LSN");
                continue;
            }
        };
        if let Err(err) = apply_payload(&data, version, relations, sync_schema, tx).await {
            error!(error = %err, "CDC apply failed");
        }
    }
    Ok(())
}

async fn apply_payload(
    payload: &[u8],
    version: u64,
    relations: &mut HashMap<u32, Relation>,
    sync_schema: &SyncSchema,
    tx: &mpsc::Sender<SourceEvent>,
) -> Result<()> {
    for message in parse_messages(payload)? {
        match message {
            PgOutput::Relation(rel) => {
                relations.insert(rel.rel_id, rel);
            }
            PgOutput::Insert { rel_id, tuple } | PgOutput::Update { rel_id, new_tuple: tuple } => {
                let Some(rel) = relations.get(&rel_id) else {
                    continue;
                };
                let Some((entity_type, source_id, entity)) =
                    sync_schema.entity_and_source_for_table(&rel.name)
                else {
                    continue;
                };
                let row = tuple_to_object(&rel.columns, &tuple);
                let owned = sync_schema.fields_owned_by(entity_type, source_id);
                let column_map = sync_schema.column_map(entity_type, source_id);
                let pk = vec![entity.identity.field.clone()];
                let (updates, _) = project_postgres_rows(
                    entity_type,
                    source_id,
                    &entity.identity.field,
                    &owned,
                    &pk,
                    vec![row],
                    Some(version),
                    &column_map,
                );
                for update in updates {
                    if tx.send(SourceEvent::Upsert(update)).await.is_err() {
                        return Ok(());
                    }
                }
            }
            PgOutput::Delete { rel_id, key_tuple } => {
                let Some(rel) = relations.get(&rel_id) else {
                    continue;
                };
                let Some((entity_type, _, entity)) =
                    sync_schema.entity_and_source_for_table(&rel.name)
                else {
                    continue;
                };
                let row = tuple_to_object(&rel.columns, &key_tuple);
                let id = row
                    .get(&entity.identity.field)
                    .or_else(|| rel.columns.first().and_then(|c| row.get(c)))
                    .map(json_id)
                    .unwrap_or_else(|| "unknown".into());
                if tx
                    .send(SourceEvent::Delete {
                        entity_type: entity_type.to_string(),
                        id,
                    })
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
            PgOutput::Begin { .. } | PgOutput::Commit { .. } | PgOutput::Other => {}
        }
    }
    Ok(())
}

fn json_id(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Number(number) => number.to_string(),
        serde_json::Value::Bool(flag) => flag.to_string(),
        serde_json::Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}
