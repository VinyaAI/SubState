//! Background Postgres poller (schema-aware).
//!
//! Every `poll_ms`, re-reads each schema-mapped postgres table, applies only
//! postgres-owned fields, deletes entities whose identity rows disappeared,
//! then publishes sequenced deltas through the hub.

use crate::hub::DeliveryHub;
use anyhow::Result;
use postgres_source::{self, project_postgres_rows, PgPool};
use schema::SyncSchema;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

pub fn spawn(
    pool: PgPool,
    pg_schema: String,
    poll_ms: u64,
    sync_schema: Arc<SyncSchema>,
    hub: Arc<DeliveryHub>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = Duration::from_millis(poll_ms.max(100));
        info!(poll_ms = interval.as_millis() as u64, "poller started");
        loop {
            tokio::time::sleep(interval).await;
            match poll_once(&pool, &pg_schema, &sync_schema, &hub).await {
                Ok(()) => {}
                Err(err) => {
                    error!(error = %err, "poll cycle failed");
                }
            }
        }
    })
}

async fn poll_once(
    pool: &PgPool,
    pg_schema: &str,
    sync_schema: &SyncSchema,
    hub: &Arc<DeliveryHub>,
) -> Result<()> {
    let mut all_transitions = Vec::new();

    for (logical_name, entity) in &sync_schema.entities {
        let Some((source_id, source)) = entity
            .sources
            .iter()
            .find(|(_, s)| s.source_type == "postgres")
        else {
            continue;
        };
        let Some(table) = source.table.as_deref() else {
            continue;
        };

        let rows = postgres_source::load_table(pool, pg_schema, table).await?;
        let owned = sync_schema.fields_owned_by(logical_name, source_id);
        let pk = vec![entity.identity.field.clone()];
        let (updates, present) = project_postgres_rows(
            logical_name,
            source_id,
            &entity.identity.field,
            &owned,
            &pk,
            rows,
        );

        {
            let engine_lock = hub.engine();
            let mut engine = engine_lock.write().await;
            match engine.apply_source_updates(updates) {
                Ok(t) => all_transitions.extend(t),
                Err(err) => {
                    error!(entity = %logical_name, error = %err, "postgres apply failed");
                    continue;
                }
            }

            let existing = engine.cds.ids_for_type(logical_name);
            for entity_id in existing {
                if !present.contains(&entity_id.id) {
                    all_transitions.extend(engine.remove_entity(logical_name, &entity_id.id));
                }
            }
        }
    }

    if !all_transitions.is_empty() {
        hub.publish_transitions(all_transitions).await;
    }
    Ok(())
}
