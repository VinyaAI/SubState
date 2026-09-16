//! Schema generation: catalogs → proposals → validated `SyncSchema` YAML.
//!
//! Discovery lives in the source crates; this crate owns heuristics and emit.

mod emit;
mod heuristics;
mod propose;

pub use emit::schema_to_yaml;
pub use heuristics::{
    is_latest_value_field, is_ordering_field, propose_attach_entity, propose_entity_key,
    propose_ordering, singularize, source_id_from_topic, table_is_eligible,
};
pub use propose::{
    attach_http_source, attach_kafka_to_entity, build_schema, default_entity_name,
    entity_from_postgres_table, kafka_only_entity, propose_kafka_attach, ConflictPolicy,
    EntityDraft, FieldDraft, KafkaAttachDraft, SourceDraft,
};

use anyhow::Result;
use schema::SyncSchema;

/// Build a schema and serialize it as sorted YAML.
pub fn generate_yaml(entities: Vec<EntityDraft>) -> Result<String> {
    let schema = build_schema(entities)?;
    schema_to_yaml(&schema)
}

/// Round-trip helper: ensure generated YAML still validates.
pub fn validate_yaml(yaml: &str) -> Result<SyncSchema> {
    SyncSchema::from_yaml_str(yaml)
}
