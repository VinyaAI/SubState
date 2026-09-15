//! Interactive terminal shell for the CDS + subscriptions.
//!
//! Inspect commands:
//! - `tables` / `ls`, `schema`, `show`, `get`
//!
//! Sync commands:
//! - `subscribe <table> key=value ...`
//! - `subscriptions` / `subs`
//! - `state <sub_id>`
//! - `unsub <sub_id>`
//!
//! Live sequenced deltas from the poller print as they arrive.

use crate::hub::{DeliveryHub, HubEvent};
use cds::Cds;
use delta::{Delta, DeltaOp};
use engine::Engine;
use serde_json::{Map, Value};
use std::io::{self, Write};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::broadcast;

const DEFAULT_SHOW_LIMIT: usize = 20;
const MAX_SHOW_LIMIT: usize = 100;
const MAX_CELL_WIDTH: usize = 40;

#[derive(Debug, PartialEq)]
pub(crate) enum Command {
    Help,
    Tables,
    Schema { table: String },
    Show { table: String, limit: usize },
    Get { table: String, id: String },
    Subscribe {
        table: String,
        where_eq: Map<String, Value>,
    },
    Subscriptions,
    State { sub_id: String },
    Unsub { sub_id: String },
    Quit,
    Empty,
}

/// Print a short load summary, then run the interactive `cds>` loop until quit.
pub async fn run(hub: Arc<DeliveryHub>) -> io::Result<()> {
    {
        let engine = hub.engine();
        let guard = engine.read().await;
        println!("{}", format_summary(&guard.cds));
        println!("Type help for commands.\n");
    }

    let mut events = hub.subscribe_events();
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();

    loop {
        print!("cds> ");
        io::stdout().flush()?;

        tokio::select! {
            biased;

            maybe_line = lines.next_line() => {
                match maybe_line? {
                    None => {
                        println!();
                        break;
                    }
                    Some(line) => {
                        match parse_command(&line) {
                            Ok(Command::Empty) => {}
                            Ok(Command::Quit) => break,
                            Ok(cmd) => {
                                let output = dispatch_cmd(&hub, &cmd).await;
                                if !output.is_empty() {
                                    println!("{output}");
                                }
                            }
                            Err(message) => println!("{message}"),
                        }
                    }
                }
            }

            event = events.recv() => {
                match event {
                    Ok(HubEvent::Delta(delta)) => {
                        println!();
                        println!("{}", format_delta(&delta));
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    Ok(())
}

async fn dispatch_cmd(hub: &DeliveryHub, cmd: &Command) -> String {
    match cmd {
        Command::Subscribe { table, where_eq } => {
            match hub.subscribe(table.clone(), where_eq.clone()).await {
                Ok((sub_id, entities)) => {
                    let mut lines = vec![
                        format!("subscribed {sub_id}"),
                        format!("  table:   {table}"),
                        format!("  where:   {}", format_where(where_eq)),
                        format!("  matches: {}", entities.len()),
                    ];
                    for entity in entities.iter().take(10) {
                        lines.push(format!("    {}", entity.id));
                    }
                    if entities.len() > 10 {
                        lines.push(format!("    ... {} more", entities.len() - 10));
                    }
                    lines.join("\n")
                }
                Err(message) => message,
            }
        }
        Command::Unsub { sub_id } => {
            if hub.unsubscribe(sub_id).await {
                format!("unsubscribed {sub_id}")
            } else {
                format!("unknown subscription '{sub_id}'")
            }
        }
        other => {
            let engine = hub.engine();
            let mut guard = engine.write().await;
            dispatch(&mut guard, other)
        }
    }
}

pub(crate) fn parse_command(line: &str) -> Result<Command, String> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(Command::Empty);
    }

    let mut parts = line.split_whitespace();
    let verb = parts.next().unwrap_or("");

    match verb {
        "help" | "?" => Ok(Command::Help),
        "tables" | "ls" => {
            if parts.next().is_some() {
                return Err("usage: tables".to_string());
            }
            Ok(Command::Tables)
        }
        "schema" => {
            let table = parts
                .next()
                .ok_or_else(|| "usage: schema <table>".to_string())?
                .to_string();
            if parts.next().is_some() {
                return Err("usage: schema <table>".to_string());
            }
            Ok(Command::Schema { table })
        }
        "show" => {
            let table = parts
                .next()
                .ok_or_else(|| "usage: show <table> [n]".to_string())?
                .to_string();
            let limit = match parts.next() {
                Some(raw) => {
                    let n: usize = raw.parse().map_err(|_| format!("invalid limit '{raw}'"))?;
                    if parts.next().is_some() {
                        return Err("usage: show <table> [n]".to_string());
                    }
                    n.clamp(1, MAX_SHOW_LIMIT)
                }
                None => DEFAULT_SHOW_LIMIT,
            };
            Ok(Command::Show { table, limit })
        }
        "get" => {
            let table = parts
                .next()
                .ok_or_else(|| "usage: get <table> <id>".to_string())?
                .to_string();
            let rest = parts.collect::<Vec<_>>().join(" ");
            if rest.is_empty() {
                return Err("usage: get <table> <id>".to_string());
            }
            Ok(Command::Get { table, id: rest })
        }
        "subscribe" => {
            let table = parts
                .next()
                .ok_or_else(|| "usage: subscribe <table> [key=value ...]".to_string())?
                .to_string();
            let mut where_eq = Map::new();
            for pair in parts {
                let (key, value) = parse_where_pair(pair)?;
                where_eq.insert(key, value);
            }
            Ok(Command::Subscribe { table, where_eq })
        }
        "subscriptions" | "subs" => {
            if parts.next().is_some() {
                return Err("usage: subscriptions".to_string());
            }
            Ok(Command::Subscriptions)
        }
        "state" => {
            let sub_id = parts
                .next()
                .ok_or_else(|| "usage: state <sub_id>".to_string())?
                .to_string();
            if parts.next().is_some() {
                return Err("usage: state <sub_id>".to_string());
            }
            Ok(Command::State { sub_id })
        }
        "unsub" => {
            let sub_id = parts
                .next()
                .ok_or_else(|| "usage: unsub <sub_id>".to_string())?
                .to_string();
            if parts.next().is_some() {
                return Err("usage: unsub <sub_id>".to_string());
            }
            Ok(Command::Unsub { sub_id })
        }
        "quit" | "exit" => Ok(Command::Quit),
        other => Err(format!(
            "unknown command '{other}'. Type help for commands."
        )),
    }
}

fn parse_where_pair(pair: &str) -> Result<(String, Value), String> {
    let Some((key, raw)) = pair.split_once('=') else {
        return Err(format!(
            "invalid filter '{pair}' (expected key=value)"
        ));
    };
    if key.is_empty() {
        return Err(format!("invalid filter '{pair}' (empty key)"));
    }
    Ok((key.to_string(), parse_filter_value(raw)))
}

/// Parse filter values: JSON bool/number/null when possible, otherwise string.
pub(crate) fn parse_filter_value(raw: &str) -> Value {
    if let Ok(value) = serde_json::from_str::<Value>(raw) {
        match value {
            Value::Bool(_) | Value::Number(_) | Value::Null => return value,
            Value::String(text) => return Value::String(text),
            _ => {}
        }
    }
    Value::String(raw.to_string())
}

fn dispatch(engine: &mut Engine, cmd: &Command) -> String {
    match cmd {
        Command::Help => format_help(),
        Command::Tables => format_tables(&engine.cds),
        Command::Schema { table } => format_schema(&engine.cds, table),
        Command::Show { table, limit } => format_show(&engine.cds, table, *limit),
        Command::Get { table, id } => format_get(&engine.cds, table, id),
        Command::Subscriptions => format_subscriptions(engine),
        Command::State { sub_id } => format_state(engine, sub_id),
        Command::Subscribe { .. } | Command::Unsub { .. } | Command::Quit | Command::Empty => {
            String::new()
        }
    }
}

fn format_where(where_eq: &Map<String, Value>) -> String {
    if where_eq.is_empty() {
        return "*".to_string();
    }
    let mut parts: Vec<_> = where_eq
        .iter()
        .map(|(k, v)| format!("{k}={}", json_display(v)))
        .collect();
    parts.sort();
    parts.join(" ")
}

fn format_subscriptions(engine: &Engine) -> String {
    let subs = engine.subscriptions();
    if subs.is_empty() {
        return "No subscriptions.".to_string();
    }
    let mut lines = Vec::new();
    for (sub, count) in subs {
        lines.push(format!(
            "{}  {}  where {}  ({} entities)",
            sub.id,
            sub.entity_type,
            format_where(&sub.where_eq),
            count
        ));
    }
    lines.join("\n")
}

fn format_state(engine: &Engine, sub_id: &str) -> String {
    let Some(sub) = engine.index.get(sub_id).cloned() else {
        return format!("unknown subscription '{sub_id}'");
    };
    let Some(user_state) = engine.user_state(sub_id) else {
        return format!("subscription '{sub_id}' has no user state");
    };

    let mut lines = vec![
        format!("{}  {}", sub.id, sub.entity_type),
        format!("where: {}", format_where(&sub.where_eq)),
        format!("entities: {}", user_state.entity_count()),
    ];

    let meta = engine
        .cds
        .catalog()
        .tables
        .iter()
        .find(|t| t.name == sub.entity_type);

    let mut items: Vec<_> = user_state.entities.iter().collect();
    items.sort_by(|a, b| a.0.id.cmp(&b.0.id));
    let total = items.len();
    let limited: Vec<_> = items.into_iter().take(DEFAULT_SHOW_LIMIT).collect();

    if limited.is_empty() {
        return lines.join("\n");
    }

    if let Some(meta) = meta {
        let columns = &meta.columns;
        let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
        let mut cells: Vec<Vec<String>> = Vec::new();
        for (_id, state) in &limited {
            let row: Vec<String> = columns
                .iter()
                .map(|column| {
                    let value = state.fields.get(column).unwrap_or(&Value::Null);
                    truncate_cell(&json_display(value))
                })
                .collect();
            for (i, cell) in row.iter().enumerate() {
                widths[i] = widths[i].max(cell.len());
            }
            cells.push(row);
        }
        lines.push(String::new());
        lines.push(
            columns
                .iter()
                .enumerate()
                .map(|(i, name)| format!("{name:<width$}", width = widths[i]))
                .collect::<Vec<_>>()
                .join("  "),
        );
        lines.push(
            widths
                .iter()
                .map(|w| "-".repeat(*w))
                .collect::<Vec<_>>()
                .join("  "),
        );
        for row in cells {
            lines.push(
                row.iter()
                    .enumerate()
                    .map(|(i, cell)| format!("{cell:<width$}", width = widths[i]))
                    .collect::<Vec<_>>()
                    .join("  "),
            );
        }
    } else {
        for (id, _) in &limited {
            lines.push(format!("  {}", id.id));
        }
    }

    if total > limited.len() {
        lines.push(format!(
            "\nshowing {} of {} entities",
            limited.len(),
            total
        ));
    }

    lines.join("\n")
}

pub(crate) fn format_delta(delta: &Delta) -> String {
    let kind = match delta.op {
        DeltaOp::Add => "ADD",
        DeltaOp::Update => "UPDATE",
        DeltaOp::Remove => "REMOVE",
    };
    let entity = format!("{}:{}", delta.entity, delta.id);
    match &delta.fields {
        Some(fields) if matches!(delta.op, DeltaOp::Add | DeltaOp::Update) => {
            let summary = fields
                .iter()
                .take(4)
                .map(|(k, v)| format!("{k}={}", json_display(v)))
                .collect::<Vec<_>>()
                .join(" ");
            format!(
                "[{}] {} seq={} {}  {}",
                kind, delta.subscription, delta.seq, entity, summary
            )
        }
        _ => format!(
            "[{}] {} seq={} {}",
            kind, delta.subscription, delta.seq, entity
        ),
    }
}

pub(crate) fn format_summary(cds: &Cds) -> String {
    let catalog = cds.catalog();
    let mut lines = vec![
        format!("Snapshot ready (schema: {})", catalog.schema),
        format!("  tables:   {}", catalog.tables.len()),
        format!("  skipped:  {}", catalog.skipped.len()),
        format!("  entities: {}", cds.entity_count()),
    ];
    for table in &catalog.tables {
        lines.push(format!("    {}  {} rows", table.name, table.row_count));
    }
    for skipped in &catalog.skipped {
        lines.push(format!(
            "    {}  skipped ({})",
            skipped.name, skipped.reason
        ));
    }
    lines.join("\n")
}

fn format_help() -> String {
    [
        "Commands:",
        "  help / ?                         Show this help",
        "  tables / ls                      List loaded tables",
        "  schema <table>                   Show columns and primary key",
        "  show <table> [n]                 Show first n rows (default 20, max 100)",
        "  get <table> <id>                 Show one entity",
        "  subscribe <entity> [key=value…]  Create a subscription (logical entity name)",
        "  subscriptions / subs             List subscriptions",
        "  state <sub_id>                   Show a subscription's user state",
        "  unsub <sub_id>                   Remove a subscription",
        "  quit / exit                      Leave the shell",
        "",
        "WebSocket: ws://BIND_ADDR/v1/sync  (default BIND_ADDR=127.0.0.1:8080)",
        "  websocat ws://127.0.0.1:8080/v1/sync",
        "  {\"type\":\"subscribe\",\"entity_type\":\"driver\",\"where\":{\"name\":\"John Doe\"}}",
        "",
        "HTTP ingest: POST http://BIND_ADDR/v1/ingest",
        "  curl -s http://127.0.0.1:8080/v1/ingest -H 'content-type: application/json' \\",
        "    -d '{\"source\":\"http\",\"entity_type\":\"driver\",\"id\":\"1\",\"fields\":{\"location\":{\"lat\":36.16,\"lng\":-86.78}},\"versions\":{\"location\":1}}'",
        "",
        "Full stack smoke: docker compose up --build && ./scripts/smoke.sh",
    ]
    .join("\n")
}

pub(crate) fn format_tables(cds: &Cds) -> String {
    let catalog = cds.catalog();
    if catalog.tables.is_empty() && catalog.skipped.is_empty() {
        return "No tables loaded.".to_string();
    }

    let mut rows: Vec<(String, String, String)> = catalog
        .tables
        .iter()
        .map(|table| {
            (
                table.name.clone(),
                format!("pk: {}", table.primary_key.join(", ")),
                format!("{} rows", table.row_count),
            )
        })
        .collect();

    for skipped in &catalog.skipped {
        rows.push((
            skipped.name.clone(),
            format!("skipped: {}", skipped.reason),
            String::new(),
        ));
    }

    let w0 = rows.iter().map(|r| r.0.len()).max().unwrap_or(0);
    let w1 = rows.iter().map(|r| r.1.len()).max().unwrap_or(0);

    rows.into_iter()
        .map(|(name, mid, right)| {
            if right.is_empty() {
                format!("{name:<w0$}  {mid}")
            } else {
                format!("{name:<w0$}  {mid:<w1$}  {right}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn format_schema(cds: &Cds, table: &str) -> String {
    let Some(meta) = cds.catalog().tables.iter().find(|t| t.name == table) else {
        return format!("unknown table '{table}'");
    };

    let mut lines = vec![
        format!("table: {}", meta.name),
        format!("primary key: {}", meta.primary_key.join(", ")),
        "columns:".to_string(),
    ];
    for column in &meta.columns {
        let marker = if meta.primary_key.iter().any(|pk| pk == column) {
            "  (pk)"
        } else {
            ""
        };
        lines.push(format!("  {column}{marker}"));
    }
    lines.join("\n")
}

pub(crate) fn format_show(cds: &Cds, table: &str, limit: usize) -> String {
    if !cds.has_entity_type(table) {
        return format!("unknown table '{table}'");
    }

    let meta = cds
        .catalog()
        .tables
        .iter()
        .find(|t| t.name == table)
        .expect("has_entity_type implies catalog entry");

    let (total, items) = cds.list(table, limit);
    if items.is_empty() {
        return format!("{table}: 0 rows");
    }

    let columns = &meta.columns;
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    let mut cells: Vec<Vec<String>> = Vec::new();

    for (_id, state) in &items {
        let row: Vec<String> = columns
            .iter()
            .map(|column| {
                let value = state.fields.get(column).unwrap_or(&Value::Null);
                truncate_cell(&json_display(value))
            })
            .collect();
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.len());
        }
        cells.push(row);
    }

    let mut lines = Vec::new();
    lines.push(
        columns
            .iter()
            .enumerate()
            .map(|(i, name)| format!("{name:<width$}", width = widths[i]))
            .collect::<Vec<_>>()
            .join("  "),
    );
    lines.push(
        widths
            .iter()
            .map(|w| "-".repeat(*w))
            .collect::<Vec<_>>()
            .join("  "),
    );
    for row in cells {
        lines.push(
            row.iter()
                .enumerate()
                .map(|(i, cell)| format!("{cell:<width$}", width = widths[i]))
                .collect::<Vec<_>>()
                .join("  "),
        );
    }

    if total > items.len() {
        lines.push(format!(
            "\nshowing {} of {} rows (use: show {table} <n>)",
            items.len(),
            total
        ));
    }

    lines.join("\n")
}

pub(crate) fn format_get(cds: &Cds, table: &str, id: &str) -> String {
    if !cds.has_entity_type(table) {
        return format!("unknown table '{table}'");
    }

    let Some(state) = cds.get(table, id) else {
        return format!("entity {table}:{id} not found");
    };

    let meta = cds
        .catalog()
        .tables
        .iter()
        .find(|t| t.name == table)
        .expect("has_entity_type implies catalog entry");

    let mut lines = vec![format!("{table}:{id}")];

    let mut seen = std::collections::HashSet::new();
    for column in &meta.columns {
        seen.insert(column.clone());
        let value = state.fields.get(column).unwrap_or(&Value::Null);
        lines.push(format!("  {column}: {}", json_display(value)));
    }
    for (key, value) in &state.fields {
        if !seen.contains(key) {
            lines.push(format!("  {key}: {}", json_display(value)));
        }
    }

    lines.join("\n")
}

fn json_display(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

fn truncate_cell(value: &str) -> String {
    if value.chars().count() <= MAX_CELL_WIDTH {
        return value.to_string();
    }
    let truncated: String = value
        .chars()
        .take(MAX_CELL_WIDTH.saturating_sub(1))
        .collect();
    format!("{truncated}…")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::DeliveryHub;
    use cds::{EntityId, EntityState, TableCatalog};
    use engine::Engine;
    use serde_json::json;

    use schema::SyncSchema;

    fn object(value: Value) -> Map<String, Value> {
        value.as_object().cloned().expect("object")
    }

    fn sample_schema() -> SyncSchema {
        SyncSchema::from_yaml_str(
            r#"
entities:
  drivers:
    identity: { field: id }
    sources:
      postgres: { type: postgres, table: drivers }
      http: { type: http }
    fields:
      id: { source: postgres }
      name: { source: postgres }
      status: { source: postgres }
      location: { source: http, mode: latest_value }
"#,
        )
        .unwrap()
    }

    fn sample_engine() -> Engine {
        let mut cds = Cds::new("public");
        cds.add_table(TableCatalog {
            name: "drivers".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec!["id".to_string(), "name".to_string(), "status".to_string()],
            row_count: 2,
        });
        cds.skip("gps_raw", "no primary key");
        cds.insert(
            EntityId {
                entity_type: "drivers".to_string(),
                id: "728".to_string(),
            },
            EntityState::from_fields(object(json!({"id": 728, "name": "Alice", "status": "available"}))),
        );
        cds.insert(
            EntityId {
                entity_type: "drivers".to_string(),
                id: "729".to_string(),
            },
            EntityState::from_fields(object(json!({"id": 729, "name": "Bob", "status": "busy"}))),
        );
        Engine::new(cds, sample_schema())
    }

    #[test]
    fn parse_subscribe_with_filters() {
        let cmd = parse_command("subscribe drivers region=nashville status=available").unwrap();
        match cmd {
            Command::Subscribe { table, where_eq } => {
                assert_eq!(table, "drivers");
                assert_eq!(where_eq.get("region"), Some(&json!("nashville")));
                assert_eq!(where_eq.get("status"), Some(&json!("available")));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn parse_filter_value_types() {
        assert_eq!(parse_filter_value("true"), json!(true));
        assert_eq!(parse_filter_value("42"), json!(42));
        assert_eq!(parse_filter_value("nashville"), json!("nashville"));
    }

    #[test]
    fn parse_aliases_and_commands() {
        assert_eq!(parse_command("help").unwrap(), Command::Help);
        assert_eq!(parse_command("subs").unwrap(), Command::Subscriptions);
        assert_eq!(
            parse_command("state sub_1").unwrap(),
            Command::State {
                sub_id: "sub_1".to_string()
            }
        );
        assert_eq!(
            parse_command("unsub sub_1").unwrap(),
            Command::Unsub {
                sub_id: "sub_1".to_string()
            }
        );
        assert_eq!(parse_command("quit").unwrap(), Command::Quit);
    }

    #[tokio::test]
    async fn subscribe_via_hub_materializes() {
        let hub = Arc::new(DeliveryHub::new(Arc::new(tokio::sync::RwLock::new(
            sample_engine(),
        ))));
        let output = dispatch_cmd(
            &hub,
            &Command::Subscribe {
                table: "drivers".to_string(),
                where_eq: object(json!({"status": "available"})),
            },
        )
        .await;
        assert!(output.contains("subscribed sub_1"));
        assert!(output.contains("matches: 1"));
        assert!(output.contains("728"));
    }

    #[test]
    fn summary_and_tables_include_loaded_and_skipped() {
        let engine = sample_engine();
        let summary = format_summary(&engine.cds);
        assert!(summary.contains("schema: public"));
        assert!(summary.contains("drivers  2 rows"));
        assert!(summary.contains("gps_raw  skipped (no primary key)"));
    }
}
