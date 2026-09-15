//! Poll follow: full-table re-read on an interval (CDC fallback).

use crate::{load_table, project_postgres_rows};
use anyhow::Result;
use schema::SyncSchema;
use source::{Follow, SourceEvent};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info};

pub struct PostgresPollFollow {
    pub pool: PgPool,
    pub pg_schema: String,
    pub poll_ms: u64,
    pub sync_schema: Arc<SyncSchema>,
}

impl Follow for PostgresPollFollow {
    fn spawn(self: Box<Self>, tx: mpsc::Sender<SourceEvent>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let interval = Duration::from_millis(self.poll_ms.max(100));
            info!(poll_ms = interval.as_millis() as u64, "postgres poll follow started");
            loop {
                tokio::time::sleep(interval).await;
                if let Err(err) = poll_once(&self.pool, &self.pg_schema, &self.sync_schema, &tx).await
                {
                    error!(error = %err, "postgres poll cycle failed");
                }
            }
        })
    }
}

async fn poll_once(
    pool: &PgPool,
    pg_schema: &str,
    sync_schema: &SyncSchema,
    tx: &mpsc::Sender<SourceEvent>,
) -> Result<()> {
    for (logical_name, entity) in &sync_schema.entities {
        let Some((source_id, source)) = entity
            .sources
            .iter()
            .find(|(_, s)| s.source_type == schema::SOURCE_POSTGRES)
        else {
            continue;
        };
        let Some(table) = source.table.as_deref() else {
            continue;
        };

        let rows = load_table(pool, pg_schema, table).await?;
        let owned = sync_schema.fields_owned_by(logical_name, source_id);
        let pk = vec![entity.identity.field.clone()];
        let (updates, present) = project_postgres_rows(
            logical_name,
            source_id,
            &entity.identity.field,
            &owned,
            &pk,
            rows,
            None,
        );

        for update in updates {
            if tx.send(SourceEvent::Upsert(update)).await.is_err() {
                return Ok(());
            }
        }

        if tx
            .send(SourceEvent::Reconcile {
                entity_type: logical_name.clone(),
                present,
            })
            .await
            .is_err()
        {
            return Ok(());
        }
    }
    Ok(())
}
