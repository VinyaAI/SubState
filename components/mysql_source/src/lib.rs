//! MySQL adapter: poll tables into [`SourceUpdate`]s.
//!
//! Enabled when the sync schema has a `mysql` source and `MYSQL_URL` is set.

use anyhow::{Context, Result};
use cds::SourceUpdate;
use schema::{SyncSchema, SOURCE_MYSQL};
use serde_json::{Map, Value};
use source::{Follow, SourceEvent};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySqlPool, Row};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub struct MySqlPollFollow {
    pub url: String,
    pub poll_ms: u64,
    pub sync_schema: Arc<SyncSchema>,
}

impl Follow for MySqlPollFollow {
    fn spawn(self: Box<Self>, tx: mpsc::Sender<SourceEvent>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let pool = match MySqlPoolOptions::new()
                .max_connections(3)
                .connect(&self.url)
                .await
            {
                Ok(pool) => pool,
                Err(err) => {
                    warn!(error = %err, "mysql connect failed");
                    return;
                }
            };
            info!(poll_ms = self.poll_ms, "mysql poll follow started");
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(self.poll_ms.max(200)));
            loop {
                interval.tick().await;
                if let Err(err) = poll_once(&pool, &self.sync_schema, &tx).await {
                    warn!(error = %err, "mysql poll cycle failed");
                }
            }
        })
    }
}

async fn poll_once(
    pool: &MySqlPool,
    sync_schema: &SyncSchema,
    tx: &mpsc::Sender<SourceEvent>,
) -> Result<()> {
    for (logical_name, entity) in &sync_schema.entities {
        let Some((source_id, source)) = entity
            .sources
            .iter()
            .find(|(_, s)| s.source_type == SOURCE_MYSQL)
        else {
            continue;
        };
        let Some(table) = source.table.as_deref() else {
            continue;
        };
        let owned = sync_schema.fields_owned_by(logical_name, source_id);
        let column_map = sync_schema.column_map(logical_name, source_id);
        let rows = load_table(pool, table).await?;
        for row in rows {
            if let Some(update) = project_mysql_row(
                logical_name,
                source_id,
                entity.identity.field(),
                &owned,
                &row,
                &column_map,
            ) {
                if tx.send(SourceEvent::Upsert(update)).await.is_err() {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

pub async fn load_table(pool: &MySqlPool, table: &str) -> Result<Vec<Map<String, Value>>> {
    let sql = format!("SELECT * FROM `{}`", table.replace('`', "``"));
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .with_context(|| format!("mysql snapshot {table}"))?;
    let mut out = Vec::new();
    for row in rows {
        let mut map = Map::new();
        for (i, col) in row.columns().iter().enumerate() {
            use sqlx::Column;
            let name = col.name().to_string();
            let value: Value = if let Ok(v) = row.try_get::<serde_json::Value, _>(i) {
                v
            } else if let Ok(v) = row.try_get::<String, _>(i) {
                Value::String(v)
            } else if let Ok(v) = row.try_get::<i64, _>(i) {
                json_number(v)
            } else if let Ok(v) = row.try_get::<f64, _>(i) {
                serde_json::Number::from_f64(v)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            } else {
                Value::Null
            };
            map.insert(name, value);
        }
        out.push(map);
    }
    Ok(out)
}

fn json_number(v: i64) -> Value {
    Value::Number(v.into())
}

/// Project one MySQL row onto owned logical fields.
pub fn project_mysql_row(
    entity_type: &str,
    source_id: &str,
    identity_field: &str,
    owned_fields: &[&str],
    row: &Map<String, Value>,
    column_map: &HashMap<String, String>,
) -> Option<SourceUpdate> {
    let id_col = column_map
        .get(identity_field)
        .map(String::as_str)
        .unwrap_or(identity_field);
    let id = row.get(id_col).or_else(|| row.get(identity_field))?;
    let id = match id {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        other => other.to_string(),
    };
    let mut fields = Map::new();
    for key in owned_fields {
        let physical = column_map.get(*key).map(String::as_str).unwrap_or(*key);
        if let Some(value) = row.get(physical).or_else(|| row.get(*key)) {
            fields.insert((*key).to_string(), value.clone());
        }
    }
    if fields.is_empty() {
        return None;
    }
    Some(SourceUpdate {
        source: source_id.to_string(),
        entity_type: entity_type.to_string(),
        id,
        fields,
        versions: HashMap::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn projects_owned_fields() {
        let owned = ["id", "name"];
        let update = project_mysql_row(
            "driver",
            "mysql",
            "id",
            &owned,
            &json!({"id": 1, "name": "Ada", "extra": true})
                .as_object()
                .cloned()
                .unwrap(),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(update.id, "1");
        assert!(!update.fields.contains_key("extra"));
    }
}
