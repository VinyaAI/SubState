//! Postgres `information_schema` catalog for snapshot and schema generation.

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};
use std::collections::HashMap;

/// One column from a base table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Foreign key from one table to another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyInfo {
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

/// Physical table metadata used by discovery and snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub primary_key: Vec<String>,
    pub foreign_keys: Vec<ForeignKeyInfo>,
}

impl TableInfo {
    /// True when the table has exactly one primary-key column.
    pub fn has_single_pk(&self) -> bool {
        self.primary_key.len() == 1
    }
}

/// Full catalog for one Postgres schema (`public`, …).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresCatalog {
    pub schema: String,
    pub tables: Vec<TableInfo>,
}

impl PostgresCatalog {
    pub fn table(&self, name: &str) -> Option<&TableInfo> {
        self.tables.iter().find(|t| t.name == name)
    }

    /// Tables that can become SubState entities (single-column primary key).
    pub fn single_pk_tables(&self) -> Vec<&TableInfo> {
        self.tables.iter().filter(|t| t.has_single_pk()).collect()
    }
}

/// Load tables, columns (with types), primary keys, and foreign keys.
pub async fn load_catalog(pool: &PgPool, schema: &str) -> Result<PostgresCatalog> {
    let table_names = discover_tables(pool, schema).await?;
    let columns = discover_columns(pool, schema).await?;
    let primary_keys = discover_primary_keys(pool, schema).await?;
    let foreign_keys = discover_foreign_keys(pool, schema).await?;

    let mut tables = Vec::with_capacity(table_names.len());
    for name in table_names {
        tables.push(TableInfo {
            columns: columns.get(&name).cloned().unwrap_or_default(),
            primary_key: primary_keys.get(&name).cloned().unwrap_or_default(),
            foreign_keys: foreign_keys.get(&name).cloned().unwrap_or_default(),
            name,
        });
    }

    Ok(PostgresCatalog {
        schema: schema.to_string(),
        tables,
    })
}

/// Table names only (used by snapshot for presence checks).
pub async fn discover_tables(pool: &PgPool, schema: &str) -> Result<Vec<String>> {
    let rows = sqlx::query(
        r#"
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = $1
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        "#,
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .context("failed to discover tables")?;

    Ok(rows
        .into_iter()
        .map(|row| row.get::<String, _>("table_name"))
        .collect())
}

/// Column names per table (legacy shape for CDS catalog columns).
pub async fn discover_column_names(
    pool: &PgPool,
    schema: &str,
) -> Result<HashMap<String, Vec<String>>> {
    let columns = discover_columns(pool, schema).await?;
    Ok(columns
        .into_iter()
        .map(|(table, cols)| {
            (
                table,
                cols.into_iter().map(|c| c.name).collect::<Vec<_>>(),
            )
        })
        .collect())
}

async fn discover_columns(
    pool: &PgPool,
    schema: &str,
) -> Result<HashMap<String, Vec<ColumnInfo>>> {
    let rows = sqlx::query(
        r#"
        SELECT table_name, column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = $1
        ORDER BY table_name, ordinal_position
        "#,
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .context("failed to discover columns")?;

    let mut columns: HashMap<String, Vec<ColumnInfo>> = HashMap::new();
    for row in rows {
        let table_name: String = row.get("table_name");
        let column_name: String = row.get("column_name");
        let data_type: String = row.get("data_type");
        let is_nullable: String = row.get("is_nullable");
        columns.entry(table_name).or_default().push(ColumnInfo {
            name: column_name,
            data_type,
            nullable: is_nullable.eq_ignore_ascii_case("YES"),
        });
    }
    Ok(columns)
}

/// Primary-key column names per table, in ordinal order.
pub async fn discover_primary_keys(
    pool: &PgPool,
    schema: &str,
) -> Result<HashMap<String, Vec<String>>> {
    let rows = sqlx::query(
        r#"
        SELECT kcu.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = $1
          AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.table_name, kcu.ordinal_position
        "#,
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .context("failed to discover primary keys")?;

    let mut keys: HashMap<String, Vec<String>> = HashMap::new();
    for row in rows {
        let table_name: String = row.get("table_name");
        let column_name: String = row.get("column_name");
        keys.entry(table_name).or_default().push(column_name);
    }
    Ok(keys)
}

async fn discover_foreign_keys(
    pool: &PgPool,
    schema: &str,
) -> Result<HashMap<String, Vec<ForeignKeyInfo>>> {
    let rows = sqlx::query(
        r#"
        SELECT
          tc.constraint_name,
          kcu.table_name AS table_name,
          kcu.column_name AS column_name,
          kcu.ordinal_position,
          ccu.table_name AS referenced_table,
          ccu.column_name AS referenced_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
        WHERE tc.table_schema = $1
          AND tc.constraint_type = 'FOREIGN KEY'
        ORDER BY kcu.table_name, tc.constraint_name, kcu.ordinal_position
        "#,
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .context("failed to discover foreign keys")?;

    // Group by (table, constraint) then assemble multi-column FKs.
    let mut grouped: HashMap<(String, String), ForeignKeyInfo> = HashMap::new();
    for row in rows {
        let constraint: String = row.get("constraint_name");
        let table_name: String = row.get("table_name");
        let column_name: String = row.get("column_name");
        let referenced_table: String = row.get("referenced_table");
        let referenced_column: String = row.get("referenced_column");
        let entry = grouped
            .entry((table_name, constraint))
            .or_insert_with(|| ForeignKeyInfo {
                columns: Vec::new(),
                referenced_table,
                referenced_columns: Vec::new(),
            });
        entry.columns.push(column_name);
        entry.referenced_columns.push(referenced_column);
    }

    let mut by_table: HashMap<String, Vec<ForeignKeyInfo>> = HashMap::new();
    for ((table, _), fk) in grouped {
        by_table.entry(table).or_default().push(fk);
    }
    for fks in by_table.values_mut() {
        fks.sort_by(|a, b| a.columns.cmp(&b.columns));
    }
    Ok(by_table)
}
