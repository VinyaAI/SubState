//! `substate init` — interactive schema discovery and generation.
//!
//! Does not boot CDS / follows / HTTP. Writes a reviewable `schema.yaml`.

use anyhow::{bail, Context, Result};
use kafka_source::{discover_topics, TopicSample};
use postgres_source::{load_catalog, PostgresCatalog, TableInfo};
use schema_gen::{
    attach_http_source, attach_kafka_to_entity, build_schema, default_entity_name,
    entity_from_postgres_table, kafka_only_entity, propose_attach_entity, propose_entity_key,
    propose_kafka_attach, schema_to_yaml, table_is_eligible, ConflictPolicy, EntityDraft,
};
use std::env;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Abstract prompts so `--defaults` and tests need no TTY.
pub trait Prompter {
    fn confirm(&mut self, message: &str, default: bool) -> Result<bool>;
    fn input(&mut self, message: &str, default: &str) -> Result<String>;
    fn select(&mut self, message: &str, items: &[String], default: usize) -> Result<usize>;
    fn multi_select(&mut self, message: &str, items: &[String], defaults: &[bool]) -> Result<Vec<usize>>;
}

/// Accept every proposal without prompting.
pub struct DefaultsPrompter;

impl Prompter for DefaultsPrompter {
    fn confirm(&mut self, _message: &str, default: bool) -> Result<bool> {
        Ok(default)
    }

    fn input(&mut self, _message: &str, default: &str) -> Result<String> {
        Ok(default.to_string())
    }

    fn select(&mut self, _message: &str, items: &[String], default: usize) -> Result<usize> {
        Ok(default.min(items.len().saturating_sub(1)))
    }

    fn multi_select(
        &mut self,
        _message: &str,
        items: &[String],
        defaults: &[bool],
    ) -> Result<Vec<usize>> {
        Ok(defaults
            .iter()
            .enumerate()
            .filter_map(|(i, on)| (*on && i < items.len()).then_some(i))
            .collect())
    }
}

/// Interactive prompts via `dialoguer`.
pub struct DialoguerPrompter;

impl Prompter for DialoguerPrompter {
    fn confirm(&mut self, message: &str, default: bool) -> Result<bool> {
        Ok(dialoguer::Confirm::new()
            .with_prompt(message)
            .default(default)
            .interact()
            .context("confirm prompt")?)
    }

    fn input(&mut self, message: &str, default: &str) -> Result<String> {
        Ok(dialoguer::Input::new()
            .with_prompt(message)
            .default(default.to_string())
            .interact_text()
            .context("input prompt")?)
    }

    fn select(&mut self, message: &str, items: &[String], default: usize) -> Result<usize> {
        if items.is_empty() {
            bail!("select prompt has no items");
        }
        Ok(dialoguer::Select::new()
            .with_prompt(message)
            .items(items)
            .default(default.min(items.len() - 1))
            .interact()
            .context("select prompt")?)
    }

    fn multi_select(
        &mut self,
        message: &str,
        items: &[String],
        defaults: &[bool],
    ) -> Result<Vec<usize>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let mut picker = dialoguer::MultiSelect::new().with_prompt(message).items(items);
        if defaults.len() == items.len() {
            picker = picker.defaults(defaults);
        }
        Ok(picker.interact().context("multi-select prompt")?)
    }
}

#[derive(Debug, Clone)]
pub struct InitOptions {
    pub out: PathBuf,
    pub defaults: bool,
    pub sample_size: usize,
}

impl InitOptions {
    pub fn from_env_and_flags(out: Option<PathBuf>, defaults: bool, sample_size: usize) -> Self {
        let out = out.unwrap_or_else(|| {
            env::var("SCHEMA_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("./schema.yaml"))
        });
        Self {
            out,
            defaults,
            sample_size,
        }
    }
}

/// Run schema init end-to-end.
pub async fn run(options: InitOptions) -> Result<()> {
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
    let pg_schema = env::var("CDS_SCHEMA").unwrap_or_else(|_| "public".to_string());

    if database_url.is_none() && kafka_brokers.is_none() {
        bail!("set DATABASE_URL and/or KAFKA_BROKERS before running `substate init`");
    }

    if options.defaults {
        let mut prompter = DefaultsPrompter;
        run_with_prompter(
            &mut prompter,
            &options,
            database_url.as_deref(),
            kafka_brokers.as_deref(),
            &pg_schema,
        )
        .await
    } else {
        let mut prompter = DialoguerPrompter;
        run_with_prompter(
            &mut prompter,
            &options,
            database_url.as_deref(),
            kafka_brokers.as_deref(),
            &pg_schema,
        )
        .await
    }
}

async fn run_with_prompter(
    prompter: &mut dyn Prompter,
    options: &InitOptions,
    database_url: Option<&str>,
    kafka_brokers: Option<&[String]>,
    pg_schema: &str,
) -> Result<()> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum WriteMode {
        Create,
        Merge,
        Overwrite,
    }

    let mut write_mode = WriteMode::Create;
    if options.out.exists() {
        if options.defaults {
            write_mode = WriteMode::Merge;
        } else {
            let items = vec![
                "Merge into existing schema (keep hand-edits where possible)".to_string(),
                "Overwrite entire file".to_string(),
                "Abort".to_string(),
            ];
            match prompter.select(
                &format!(
                    "Output file {} already exists. What should we do?",
                    options.out.display()
                ),
                &items,
                0,
            )? {
                0 => write_mode = WriteMode::Merge,
                1 => write_mode = WriteMode::Overwrite,
                _ => bail!("aborted — existing schema left unchanged"),
            }
        }
    }

    let mut entities: Vec<EntityDraft> = Vec::new();
    let mut skipped_tables: Vec<String> = Vec::new();

    if let Some(url) = database_url {
        println!("Scanning Postgres ({pg_schema})...");
        let pool = postgres_source::connect(url).await?;
        let catalog = load_catalog(&pool, pg_schema).await?;
        let (drafts, skipped) = select_postgres_entities(prompter, &catalog)?;
        entities.extend(drafts);
        skipped_tables = skipped;
        println!(
            "Selected {} postgres entit{}",
            entities.len(),
            if entities.len() == 1 { "y" } else { "ies" }
        );
    }

    if let Some(brokers) = kafka_brokers {
        println!("Scanning Kafka...");
        let samples = discover_topics(brokers, options.sample_size).await?;
        if samples.is_empty() {
            println!("No topics found (or all were internal).");
        } else {
            attach_kafka_topics(prompter, &mut entities, &samples)?;
        }
    }

    if entities.is_empty() {
        bail!("no entities selected — nothing to write");
    }

    let add_http = prompter.confirm("Add an `http` ingest source to every entity?", false)?;
    if add_http {
        for entity in &mut entities {
            attach_http_source(entity, "http");
        }
    }

    let mut schema = build_schema(entities).context("generated schema failed validation")?;
    if write_mode == WriteMode::Merge {
        if let Ok(existing) = schema::SyncSchema::load_path(&options.out) {
            schema = schema_gen::merge_schemas(existing, schema);
            schema.validate().context("merged schema failed validation")?;
        }
    }
    let yaml = schema_to_yaml(&schema)?;

    if let Some(parent) = options.out.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create directory {}", parent.display()))?;
        }
    }
    std::fs::write(&options.out, &yaml)
        .with_context(|| format!("write {}", options.out.display()))?;

    println!();
    println!("Wrote {}", options.out.display());
    if write_mode == WriteMode::Merge {
        println!("(merged into existing schema)");
    }
    if !skipped_tables.is_empty() {
        println!(
            "Skipped tables (no primary key): {}",
            skipped_tables.join(", ")
        );
    }
    println!();
    println!("Next steps:");
    println!("  1. Review {}", options.out.display());
    println!(
        "  2. Ensure SCHEMA_PATH points at it (e.g. SCHEMA_PATH={})",
        display_schema_path(&options.out)
    );
    println!("  3. cargo run -p substate-cli -- serve");
    let _ = io::stdout().flush();
    Ok(())
}

fn display_schema_path(path: &Path) -> String {
    path.display().to_string()
}

fn select_postgres_entities(
    prompter: &mut dyn Prompter,
    catalog: &PostgresCatalog,
) -> Result<(Vec<EntityDraft>, Vec<String>)> {
    let mut eligible = Vec::new();
    let mut skipped = Vec::new();
    for table in &catalog.tables {
        if table_is_eligible(table) {
            eligible.push(table);
        } else {
            skipped.push(table.name.clone());
            let reason = if table.primary_key.is_empty() {
                "no primary key"
            } else {
                "composite primary key"
            };
            println!("  skip {}.{} ({reason})", catalog.schema, table.name);
        }
    }

    if eligible.is_empty() {
        println!("No single-PK tables found in schema '{}'.", catalog.schema);
        return Ok((Vec::new(), skipped));
    }

    let labels: Vec<String> = eligible
        .iter()
        .map(|t| {
            let pk = t.primary_key.join(",");
            format!("{} (pk: {pk}, {} cols)", t.name, t.columns.len())
        })
        .collect();
    let defaults: Vec<bool> = vec![true; labels.len()];
    let chosen = prompter.multi_select("Postgres tables to include", &labels, &defaults)?;

    let mut drafts = Vec::new();
    for idx in chosen {
        let table: &TableInfo = eligible[idx];
        let default_name = default_entity_name(table);
        let name = prompter.input(
            &format!("Logical entity name for table '{}'", table.name),
            &default_name,
        )?;
        let name = name.trim();
        if name.is_empty() {
            bail!("entity name cannot be empty");
        }
        drafts.push(entity_from_postgres_table(table, name)?);
    }
    Ok((drafts, skipped))
}

fn attach_kafka_topics(
    prompter: &mut dyn Prompter,
    entities: &mut Vec<EntityDraft>,
    samples: &[TopicSample],
) -> Result<()> {
    let entity_names: Vec<String> = entities.iter().map(|e| e.name.clone()).collect();
    let entity_idents: Vec<(String, String)> = entities
        .iter()
        .map(|e| {
            (
                e.name.clone(),
                e.identity_fields.first().cloned().unwrap_or_default(),
            )
        })
        .collect();

    for sample in samples {
        println!();
        println!(
            "Topic '{}' — {} JSON sample(s), {} non-JSON, fields: {}",
            sample.topic,
            sample.sample_count,
            sample.non_json_count,
            if sample.fields.is_empty() {
                "(none)".to_string()
            } else {
                sample
                    .fields
                    .iter()
                    .map(|f| format!("{}:{}", f.name, f.inferred_type))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        );

        if sample.fields.is_empty() {
            println!("  (empty or non-JSON — skipping)");
            continue;
        }

        let proposed_entity = propose_attach_entity(&sample.topic, &entity_names);
        let proposed_key =
            propose_entity_key(&sample.fields, &entity_idents).unwrap_or_else(|| "id".into());

        let mut actions = vec![
            "Skip this topic".to_string(),
            "Create kafka-only entity".to_string(),
        ];
        for name in &entity_names {
            actions.push(format!("Attach to entity '{name}'"));
        }

        let default_idx = match &proposed_entity {
            Some(name) => entity_names
                .iter()
                .position(|n| n == name)
                .map(|i| i + 2)
                .unwrap_or(0),
            None => 0,
        };

        let choice = prompter.select(
            &format!(
                "What to do with topic '{}'? (proposed key: {proposed_key})",
                sample.topic
            ),
            &actions,
            default_idx,
        )?;

        if choice == 0 {
            continue;
        }

        let entity_key = prompter.input("Kafka entity_key field", &proposed_key)?;
        let entity_key = entity_key.trim().to_string();
        if entity_key.is_empty() {
            bail!("entity_key cannot be empty");
        }
        if !sample.fields.iter().any(|f| f.name == entity_key) {
            println!("  warning: '{entity_key}' was not seen in sampled payloads");
        }

        let attach = propose_kafka_attach(sample, &entity_key);

        if choice == 1 {
            let default_name = schema_gen::singularize(&sample.topic.replace('-', "_"));
            let name = prompter.input("Kafka-only entity name", &default_name)?;
            let name = name.trim().to_string();
            if name.is_empty() {
                bail!("entity name cannot be empty");
            }
            entities.push(kafka_only_entity(&attach, &name)?);
            continue;
        }

        let entity_idx = choice - 2;
        let entity = &mut entities[entity_idx];

        let conflict = if attach.field_names.iter().any(|f| {
            entity.fields.iter().any(|existing| &existing.name == f)
        }) {
            let options = vec![
                "Keep existing (Postgres) ownership".to_string(),
                "Prefer Kafka for conflicting names".to_string(),
                "Skip conflicting Kafka fields".to_string(),
            ];
            match prompter.select(
                &format!(
                    "Topic '{}' overlaps field names on '{}'. Conflict policy?",
                    sample.topic, entity.name
                ),
                &options,
                0,
            )? {
                0 => ConflictPolicy::KeepExisting,
                1 => ConflictPolicy::PreferKafka,
                _ => ConflictPolicy::Skip,
            }
        } else {
            ConflictPolicy::KeepExisting
        };

        attach_kafka_to_entity(entity, &attach, conflict)?;
        println!(
            "  attached '{}' → entity '{}' as source '{}'",
            sample.topic, entity.name, attach.source_id
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use postgres_source::{ColumnInfo, TableInfo};
    use schema_gen::{entity_from_postgres_table, generate_yaml};

    #[test]
    fn defaults_prompter_accepts_proposals() {
        let mut p = DefaultsPrompter;
        assert!(p.confirm("ok?", true).unwrap());
        assert!(!p.confirm("ok?", false).unwrap());
        assert_eq!(p.input("name", "driver").unwrap(), "driver");
        assert_eq!(
            p.select("pick", &["a".into(), "b".into()], 1).unwrap(),
            1
        );
        assert_eq!(
            p.multi_select("pick", &["a".into(), "b".into()], &[true, false])
                .unwrap(),
            vec![0]
        );
    }

    #[test]
    fn generated_fixture_yaml_validates() {
        let table = TableInfo {
            name: "drivers".into(),
            columns: vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: "integer".into(),
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".into(),
                    data_type: "text".into(),
                    nullable: false,
                },
                ColumnInfo {
                    name: "status".into(),
                    data_type: "text".into(),
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
            foreign_keys: vec![],
        };
        let entity = entity_from_postgres_table(&table, "driver").unwrap();
        let yaml = generate_yaml(vec![entity]).unwrap();
        schema::SyncSchema::from_yaml_str(&yaml).unwrap();
        assert!(yaml.contains("driver:"));
        assert!(yaml.contains("table: drivers"));
    }
}
