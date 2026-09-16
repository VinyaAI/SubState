//! Postgres discovery + schema-aware bootstrap into the CDS.
//!
//! Only fields owned by the postgres source (per sync schema) are loaded.
//! Logical entity types come from the schema, not raw table names.

pub mod catalog;
pub mod cdc;
pub mod config;
pub mod pgoutput;
pub mod poll;

pub use catalog::{
    load_catalog, ColumnInfo, ForeignKeyInfo, PostgresCatalog, TableInfo,
};
pub use config::Config;
pub use sqlx::PgPool;

use anyhow::{bail, Context, Result};
use cds::{entity_id, Cds, SourceUpdate, TableCatalog};
use schema::SyncSchema;
use serde_json::{Map, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};

/// Open a small connection pool. TLS is handled by sqlx's rustls feature.
pub async fn connect(database_url: &str) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .context("failed to connect to Postgres")
}

/// Discover physical tables and bootstrap logical entities from the sync schema.
pub async fn snapshot(
    pool: &PgPool,
    config: &Config,
    sync_schema: &SyncSchema,
) -> Result<Cds> {
    let pg_catalog = catalog::load_catalog(pool, &config.schema).await?;
    let present: HashSet<&str> = pg_catalog
        .tables
        .iter()
        .map(|t| t.name.as_str())
        .collect();

    info!(
        schema = %config.schema,
        tables = pg_catalog.tables.len(),
        entities = sync_schema.entities.len(),
        "discovered base tables"
    );

    let mut cds = Cds::new(config.schema.clone());

    for (logical_name, entity) in &sync_schema.entities {
        let Some((source_id, source)) = entity
            .sources
            .iter()
            .find(|(_, s)| s.source_type == "postgres")
        else {
            // Entity has no postgres source — catalog it empty for HTTP-only entities.
            let field_names: Vec<String> = entity.fields.keys().cloned().collect();
            cds.add_table(TableCatalog {
                name: logical_name.clone(),
                primary_key: vec![entity.identity.field.clone()],
                columns: field_names,
                row_count: 0,
            });
            continue;
        };

        let table = source
            .table
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("postgres source missing table"))?;

        if !present.contains(table) {
            warn!(
                entity = %logical_name,
                table,
                "schema postgres table not found in database"
            );
            cds.skip(
                logical_name.clone(),
                format!("postgres table '{table}' not found"),
            );
            continue;
        }

        let pk = pg_catalog
            .table(table)
            .map(|t| t.primary_key.clone())
            .unwrap_or_default();
        if pk.is_empty() {
            warn!(table, "skipping table without primary key");
            cds.skip(logical_name.clone(), "no primary key");
            continue;
        }

        let owned = sync_schema.fields_owned_by(logical_name, source_id);
        let rows = load_table(pool, &config.schema, table).await?;
        let (updates, _) = project_postgres_rows(
            logical_name,
            source_id,
            &entity.identity.field,
            &owned,
            &pk,
            rows,
            None,
        );

        for update in updates {
            let _ = cds.apply_source_update(update, owned.as_slice());
        }

        let table_columns: Vec<String> = pg_catalog
            .table(table)
            .map(|t| t.columns.iter().map(|c| c.name.clone()).collect())
            .unwrap_or_default();
        let mut catalog_columns: Vec<String> = entity.fields.keys().cloned().collect();
        catalog_columns.sort();
        let row_count = cds.ids_for_type(logical_name).len();
        cds.add_table(TableCatalog {
            name: logical_name.clone(),
            primary_key: vec![entity.identity.field.clone()],
            columns: if catalog_columns.is_empty() {
                table_columns
            } else {
                catalog_columns
            },
            row_count,
        });

        info!(
            entity = %logical_name,
            table,
            rows = row_count,
            "loaded logical entity from postgres"
        );
    }

    // Optional CDS_TABLES warn if set and unused under schema mode.
    if let Some(allowlist) = &config.tables {
        for table in allowlist {
            if sync_schema.entity_for_table(table).is_none() {
                warn!(
                    table,
                    "CDS_TABLES entry is not referenced by sync schema postgres sources"
                );
            }
        }
    }

    if cds.catalog().tables.is_empty() && cds.catalog().skipped.is_empty() {
        bail!("sync schema produced no entities; check SCHEMA_PATH and postgres table names");
    }

    info!(
        schema = %config.schema,
        entities = cds.catalog().tables.len(),
        skipped = cds.catalog().skipped.len(),
        loaded = cds.entity_count(),
        "CDS snapshot complete"
    );

    Ok(cds)
}

/// Empty CDS catalog for schemas with no Postgres source (HTTP / Kafka only).
pub fn catalog_only(schema_name: impl Into<String>, sync_schema: &SyncSchema) -> Cds {
    let mut cds = Cds::new(schema_name);
    for (logical_name, entity) in &sync_schema.entities {
        let field_names: Vec<String> = entity.fields.keys().cloned().collect();
        cds.add_table(TableCatalog {
            name: logical_name.clone(),
            primary_key: vec![entity.identity.field.clone()],
            columns: field_names,
            row_count: 0,
        });
    }
    cds
}

/// Project physical rows into postgres-owned [`SourceUpdate`]s for one logical entity.
///
/// `version` is applied to every field when set (CDC LSN). Otherwise versions stay empty
/// and the CDS assigns `stored+1`.
///
/// Returns `(updates, present_entity_ids)`.
pub fn project_postgres_rows(
    logical_entity: &str,
    source_id: &str,
    identity_field: &str,
    owned_fields: &[&str],
    primary_key: &[String],
    rows: Vec<Map<String, Value>>,
    version: Option<u64>,
) -> (Vec<SourceUpdate>, HashSet<String>) {
    let owned: HashSet<&str> = owned_fields.iter().copied().collect();
    let mut updates = Vec::new();
    let mut present = HashSet::new();

    for row in rows {
        let id = if owned.contains(identity_field) {
            // Prefer schema identity field when present on the row.
            if row.contains_key(identity_field) {
                json_id(row.get(identity_field).unwrap())
            } else {
                entity_id(primary_key, &row)
            }
        } else {
            entity_id(primary_key, &row)
        };
        present.insert(id.clone());

        let mut fields = Map::new();
        for key in &owned {
            if let Some(value) = row.get(*key) {
                fields.insert((*key).to_string(), value.clone());
            }
        }
        // Always include identity in fields when available on the row under PK name.
        if !fields.contains_key(identity_field) {
            if let Some(value) = row.get(identity_field) {
                fields.insert(identity_field.to_string(), value.clone());
            } else if primary_key.len() == 1 {
                if let Some(value) = row.get(&primary_key[0]) {
                    fields.insert(identity_field.to_string(), value.clone());
                }
            }
        }

        let versions = match version {
            Some(v) => fields.keys().cloned().map(|k| (k, v)).collect(),
            None => HashMap::new(),
        };

        updates.push(SourceUpdate {
            source: source_id.to_string(),
            entity_type: logical_entity.to_string(),
            id,
            fields,
            versions,
        });
    }

    (updates, present)
}

fn json_id(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

/// Load every row of one table as a JSON object via `to_jsonb`.
pub async fn load_table(
    pool: &PgPool,
    schema: &str,
    table: &str,
) -> Result<Vec<Map<String, Value>>> {
    let sql = format!(
        "SELECT to_jsonb(t) AS row FROM {}.{} t",
        quote_ident(schema),
        quote_ident(table)
    );

    let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to snapshot table {schema}.{table}"))?;

    let mut entities = Vec::with_capacity(rows.len());
    for row in rows {
        let value: Value = row.try_get("row")?;
        let object = value
            .as_object()
            .cloned()
            .with_context(|| format!("row in {schema}.{table} was not a JSON object"))?;
        entities.push(object);
    }
    Ok(entities)
}

pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::{project_postgres_rows, quote_ident};
    use serde_json::json;

    #[test]
    fn quote_ident_escapes_quotes() {
        assert_eq!(quote_ident("drivers"), "\"drivers\"");
        assert_eq!(quote_ident("weird\"name"), "\"weird\"\"name\"");
    }

    #[test]
    fn project_rows_keeps_only_owned_fields() {
        let owned = ["id", "name", "status"];
        let (updates, present) = project_postgres_rows(
            "driver",
            "postgres",
            "id",
            &owned,
            &["id".to_string()],
            vec![
                json!({"id": 1, "name": "Alice", "status": "available", "extra": true})
                    .as_object()
                    .cloned()
                    .unwrap(),
            ],
            Some(99),
        );
        assert_eq!(present.len(), 1);
        assert_eq!(updates.len(), 1);
        assert!(!updates[0].fields.contains_key("extra"));
        assert_eq!(updates[0].fields["name"], json!("Alice"));
        assert_eq!(updates[0].entity_type, "driver");
        assert_eq!(updates[0].versions.get("name"), Some(&99));
    }
}
