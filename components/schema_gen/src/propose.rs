//! In-memory drafts that become a validated [`SyncSchema`].

use crate::heuristics::{
    is_latest_value_field, is_ordering_field, propose_ordering, singularize, source_id_from_topic,
};
use anyhow::{bail, Result};
use kafka_source::TopicSample;
use postgres_source::TableInfo;
use schema::{
    EntityDef, FieldDef, FieldMode, IdentityDef, SourceDef, SyncSchema, SOURCE_HTTP, SOURCE_KAFKA,
    SOURCE_POSTGRES,
};
use std::collections::HashMap;

/// How to resolve a field name that already exists on an entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictPolicy {
    /// Keep the existing (usually Postgres) owner.
    KeepExisting,
    /// Let the new Kafka source take ownership.
    PreferKafka,
    /// Skip adding the conflicting Kafka field.
    Skip,
}

#[derive(Debug, Clone)]
pub struct FieldDraft {
    pub name: String,
    pub source: String,
    pub mode: FieldMode,
    pub ordering: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SourceDraft {
    pub id: String,
    pub source_type: String,
    pub table: Option<String>,
    pub topic: Option<String>,
    pub entity_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EntityDraft {
    pub name: String,
    pub identity_fields: Vec<String>,
    pub sources: Vec<SourceDraft>,
    pub fields: Vec<FieldDraft>,
}

#[derive(Debug, Clone)]
pub struct KafkaAttachDraft {
    pub topic: String,
    pub source_id: String,
    pub entity_key: String,
    pub field_names: Vec<String>,
    pub ordering: Option<String>,
}

/// Build an entity draft from one Postgres table (single or composite PK).
pub fn entity_from_postgres_table(table: &TableInfo, entity_name: &str) -> Result<EntityDraft> {
    if table.primary_key.is_empty() {
        bail!("table '{}' has no primary key", table.name);
    }
    let identity_fields = table.primary_key.clone();
    let mut fields = Vec::new();
    for col in &table.columns {
        fields.push(FieldDraft {
            name: col.name.clone(),
            source: "postgres".into(),
            mode: FieldMode::Transactional,
            ordering: None,
        });
    }
    for pk in &identity_fields {
        if !fields.iter().any(|f| f.name == *pk) {
            bail!(
                "primary key '{pk}' not present in columns for table '{}'",
                table.name
            );
        }
    }
    Ok(EntityDraft {
        name: entity_name.to_string(),
        identity_fields,
        sources: vec![SourceDraft {
            id: "postgres".into(),
            source_type: SOURCE_POSTGRES.into(),
            table: Some(table.name.clone()),
            topic: None,
            entity_key: None,
        }],
        fields,
    })
}

/// Suggested kafka attach from a topic sample (caller still confirms).
pub fn propose_kafka_attach(
    sample: &TopicSample,
    entity_key: &str,
) -> KafkaAttachDraft {
    let ordering = propose_ordering(&sample.fields);
    let field_names: Vec<String> = sample
        .fields
        .iter()
        .map(|f| f.name.clone())
        .filter(|name| name != entity_key && !is_ordering_field(name))
        .collect();
    KafkaAttachDraft {
        topic: sample.topic.clone(),
        source_id: source_id_from_topic(&sample.topic),
        entity_key: entity_key.to_string(),
        field_names,
        ordering,
    }
}

/// Attach a Kafka source onto an existing entity draft.
pub fn attach_kafka_to_entity(
    entity: &mut EntityDraft,
    attach: &KafkaAttachDraft,
    conflict: ConflictPolicy,
) -> Result<()> {
    if entity.sources.iter().any(|s| s.id == attach.source_id) {
        bail!(
            "entity '{}' already has source '{}'",
            entity.name,
            attach.source_id
        );
    }
    if attach.field_names.is_empty() {
        bail!(
            "topic '{}' has no payload fields to attach (besides entity key / ordering)",
            attach.topic
        );
    }
    entity.sources.push(SourceDraft {
        id: attach.source_id.clone(),
        source_type: SOURCE_KAFKA.into(),
        table: None,
        topic: Some(attach.topic.clone()),
        entity_key: Some(attach.entity_key.clone()),
    });

    for name in &attach.field_names {
        if let Some(existing) = entity.fields.iter_mut().find(|f| &f.name == name) {
            match conflict {
                ConflictPolicy::KeepExisting | ConflictPolicy::Skip => continue,
                ConflictPolicy::PreferKafka => {
                    existing.source = attach.source_id.clone();
                    existing.mode = if is_latest_value_field(name) {
                        FieldMode::LatestValue
                    } else {
                        FieldMode::Transactional
                    };
                    existing.ordering = if existing.mode == FieldMode::LatestValue {
                        attach.ordering.clone()
                    } else {
                        None
                    };
                }
            }
            continue;
        }

        let mode = if is_latest_value_field(name) {
            FieldMode::LatestValue
        } else {
            FieldMode::Transactional
        };
        entity.fields.push(FieldDraft {
            name: name.clone(),
            source: attach.source_id.clone(),
            mode,
            ordering: if mode == FieldMode::LatestValue {
                attach.ordering.clone()
            } else {
                None
            },
        });
    }

    Ok(())
}

/// Create a kafka-only entity (no postgres).
pub fn kafka_only_entity(attach: &KafkaAttachDraft, entity_name: &str) -> Result<EntityDraft> {
    let mut fields = Vec::new();
    fields.push(FieldDraft {
        name: attach.entity_key.clone(),
        source: attach.source_id.clone(),
        mode: FieldMode::Transactional,
        ordering: None,
    });
    for name in &attach.field_names {
        let mode = if is_latest_value_field(name) {
            FieldMode::LatestValue
        } else {
            FieldMode::Transactional
        };
        fields.push(FieldDraft {
            name: name.clone(),
            source: attach.source_id.clone(),
            mode,
            ordering: if mode == FieldMode::LatestValue {
                attach.ordering.clone()
            } else {
                None
            },
        });
    }
    Ok(EntityDraft {
        name: entity_name.to_string(),
        identity_fields: vec![attach.entity_key.clone()],
        sources: vec![SourceDraft {
            id: attach.source_id.clone(),
            source_type: SOURCE_KAFKA.into(),
            table: None,
            topic: Some(attach.topic.clone()),
            entity_key: Some(attach.entity_key.clone()),
        }],
        fields,
    })
}

/// Optionally add an HTTP ingest source to every entity (or one entity).
pub fn attach_http_source(entity: &mut EntityDraft, source_id: &str) {
    if entity.sources.iter().any(|s| s.id == source_id) {
        return;
    }
    entity.sources.push(SourceDraft {
        id: source_id.to_string(),
        source_type: SOURCE_HTTP.into(),
        table: None,
        topic: None,
        entity_key: None,
    });
}

/// Convert drafts into a validated [`SyncSchema`].
pub fn build_schema(entities: Vec<EntityDraft>) -> Result<SyncSchema> {
    let mut map = HashMap::new();
    for entity in entities {
        if map.contains_key(&entity.name) {
            bail!("duplicate entity name '{}'", entity.name);
        }
        let mut sources = HashMap::new();
        for source in entity.sources {
            sources.insert(
                source.id,
                SourceDef {
                    source_type: source.source_type,
                    table: source.table,
                    topic: source.topic,
                    entity_key: source.entity_key,
                },
            );
        }
        let mut fields = HashMap::new();
        for field in entity.fields {
            let flush_ms = if field.mode == FieldMode::LatestValue {
                Some(100)
            } else {
                None
            };
            fields.insert(
                field.name,
                FieldDef {
                    source: field.source,
                    mode: field.mode,
                    ordering: field.ordering,
                    flush_ms,
                    ttl_ms: None,
                    column: None,
                    path: None,
                },
            );
        }
        map.insert(
            entity.name.clone(),
            EntityDef {
                identity: IdentityDef::composite(entity.identity_fields),
                sources,
                fields,
                relations: HashMap::new(),
            },
        );
    }
    let schema = SyncSchema { entities: map };
    schema.validate()?;
    Ok(schema)
}

/// Merge newly discovered schema into an existing hand-edited schema.
///
/// Existing entities keep their fields/modes/relations/remaps. New entities and
/// new fields from `incoming` are added. Incoming never deletes existing keys.
pub fn merge_schemas(mut existing: SyncSchema, incoming: SyncSchema) -> SyncSchema {
    for (name, entity) in incoming.entities {
        match existing.entities.get_mut(&name) {
            None => {
                existing.entities.insert(name, entity);
            }
            Some(dest) => {
                for (sid, source) in entity.sources {
                    dest.sources.entry(sid).or_insert(source);
                }
                for (fname, field) in entity.fields {
                    dest.fields.entry(fname).or_insert(field);
                }
                for (rname, rel) in entity.relations {
                    dest.relations.entry(rname).or_insert(rel);
                }
            }
        }
    }
    existing
}

/// Default entity name for a physical table.
pub fn default_entity_name(table: &TableInfo) -> String {
    singularize(&table.name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_source::InferredField;
    use postgres_source::{ColumnInfo, TableInfo};

    fn drivers_table() -> TableInfo {
        TableInfo {
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
        }
    }

    #[test]
    fn postgres_entity_and_kafka_attach() {
        let table = drivers_table();
        let mut entity = entity_from_postgres_table(&table, "driver").unwrap();
        let sample = TopicSample {
            topic: "driver-locations".into(),
            fields: vec![
                InferredField {
                    name: "driver_id".into(),
                    inferred_type: "number".into(),
                },
                InferredField {
                    name: "location".into(),
                    inferred_type: "object".into(),
                },
                InferredField {
                    name: "sequence".into(),
                    inferred_type: "number".into(),
                },
                InferredField {
                    name: "status".into(),
                    inferred_type: "string".into(),
                },
            ],
            sample_count: 2,
            non_json_count: 0,
        };
        let attach = propose_kafka_attach(&sample, "driver_id");
        attach_kafka_to_entity(&mut entity, &attach, ConflictPolicy::KeepExisting).unwrap();

        let schema = build_schema(vec![entity]).unwrap();
        assert!(schema.has_entity("driver"));
        assert_eq!(schema.field_authority("driver", "name"), Some("postgres"));
        assert_eq!(schema.field_authority("driver", "location"), Some("driver_locations"));
        // status collision kept on postgres
        assert_eq!(schema.field_authority("driver", "status"), Some("postgres"));
        let location = schema.entity("driver").unwrap().fields.get("location").unwrap();
        assert_eq!(location.mode, FieldMode::LatestValue);
        assert_eq!(location.ordering.as_deref(), Some("sequence"));
    }

    #[test]
    fn prefer_kafka_overrides_collision() {
        let table = drivers_table();
        let mut entity = entity_from_postgres_table(&table, "driver").unwrap();
        let sample = TopicSample {
            topic: "driver-status".into(),
            fields: vec![
                InferredField {
                    name: "driver_id".into(),
                    inferred_type: "number".into(),
                },
                InferredField {
                    name: "status".into(),
                    inferred_type: "string".into(),
                },
            ],
            sample_count: 1,
            non_json_count: 0,
        };
        let attach = propose_kafka_attach(&sample, "driver_id");
        attach_kafka_to_entity(&mut entity, &attach, ConflictPolicy::PreferKafka).unwrap();
        let schema = build_schema(vec![entity]).unwrap();
        assert_eq!(
            schema.field_authority("driver", "status"),
            Some("driver_status")
        );
    }

    #[test]
    fn empty_topic_attach_errors() {
        let table = drivers_table();
        let mut entity = entity_from_postgres_table(&table, "driver").unwrap();
        let attach = KafkaAttachDraft {
            topic: "empty".into(),
            source_id: "empty".into(),
            entity_key: "driver_id".into(),
            field_names: vec![],
            ordering: None,
        };
        let err = attach_kafka_to_entity(&mut entity, &attach, ConflictPolicy::KeepExisting)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no payload fields"), "{err}");
    }

    #[test]
    fn missing_entity_key_falls_back() {
        let fields = vec![InferredField {
            name: "location".into(),
            inferred_type: "object".into(),
        }];
        assert_eq!(
            crate::propose_entity_key(&fields, &[("driver".into(), "id".into())]),
            None
        );
    }
}
