//! Sync schema: logical entities, sources, and field authority.
//!
//! The CDS merge path consults this contract so multiple backends can contribute
//! fields to one entity without wiping each other. Automatic discovery lives in
//! `schema_gen` / `substate init`; this crate loads, validates, and serializes YAML.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SyncSchema {
    pub entities: HashMap<String, EntityDef>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EntityDef {
    pub identity: IdentityDef,
    pub sources: HashMap<String, SourceDef>,
    pub fields: HashMap<String, FieldDef>,
    /// One-hop relations (local FK → remote entity).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub relations: HashMap<String, RelationDef>,
}

/// Accepts either `field: id` or `fields: [a, b]` in YAML.
#[derive(Debug, Clone)]
pub struct IdentityDef {
    fields: Vec<String>,
}

impl Serialize for IdentityDef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        if self.fields.len() == 1 {
            map.serialize_entry("field", &self.fields[0])?;
        } else {
            map.serialize_entry("fields", &self.fields)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for IdentityDef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            field: Option<String>,
            fields: Option<Vec<String>>,
        }
        let raw = Raw::deserialize(deserializer)?;
        if let Some(fields) = raw.fields {
            if fields.is_empty() {
                return Err(serde::de::Error::custom("identity.fields must not be empty"));
            }
            return Ok(IdentityDef { fields });
        }
        if let Some(field) = raw.field {
            return Ok(IdentityDef {
                fields: vec![field],
            });
        }
        Err(serde::de::Error::custom(
            "identity requires `field` or `fields`",
        ))
    }
}

impl IdentityDef {
    pub fn single(field: impl Into<String>) -> Self {
        Self {
            fields: vec![field.into()],
        }
    }

    pub fn composite(fields: Vec<String>) -> Self {
        Self { fields }
    }

    pub fn field_names(&self) -> &[String] {
        &self.fields
    }

    /// First identity field name (single-PK convenience).
    pub fn field(&self) -> &str {
        self.fields.first().map(String::as_str).unwrap_or("")
    }
}

/// One-hop FK relation from this entity to another.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RelationDef {
    pub entity: String,
    pub local: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceDef {
    #[serde(rename = "type")]
    pub source_type: String,
    /// Physical table name when `source_type` is `postgres`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// Topic name when `source_type` is `kafka`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    /// Payload field that holds the logical entity id when `source_type` is `kafka`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity_key: Option<String>,
}

/// One Kafka topic mapped onto a logical entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaBinding {
    pub source_id: String,
    pub entity_type: String,
    pub topic: String,
    pub entity_key: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FieldDef {
    /// Authoritative source id (key under `sources`).
    pub source: String,
    #[serde(default = "default_mode")]
    pub mode: FieldMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordering: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_ms: Option<u64>,
    /// Drop latest-value field when older than this many ms (engine TTL sweep).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    /// Physical Postgres column when it differs from the logical field name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    /// Physical Kafka/HTTP JSON path (top-level key) when it differs from the logical name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

fn default_mode() -> FieldMode {
    FieldMode::Transactional
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldMode {
    Transactional,
    LatestValue,
}

pub const SOURCE_POSTGRES: &str = "postgres";
pub const SOURCE_KAFKA: &str = "kafka";
pub const SOURCE_HTTP: &str = "http";
pub const SOURCE_MYSQL: &str = "mysql";
pub const SOURCE_MONGODB: &str = "mongodb";

impl SyncSchema {
    pub fn from_yaml_str(yaml: &str) -> Result<Self> {
        let schema: SyncSchema = serde_yaml::from_str(yaml).context("invalid schema YAML")?;
        schema.validate()?;
        Ok(schema)
    }

    pub fn load_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read schema at {}", path.display()))?;
        Self::from_yaml_str(&text)
            .with_context(|| format!("failed to parse schema at {}", path.display()))
    }

    pub fn validate(&self) -> Result<()> {
        if self.entities.is_empty() {
            bail!("schema must declare at least one entity");
        }
        for (name, entity) in &self.entities {
            if entity.sources.is_empty() {
                bail!("entity '{name}' has no sources");
            }
            if entity.fields.is_empty() {
                bail!("entity '{name}' has no fields");
            }
            let identity_names = entity.identity.field_names();
            if identity_names.is_empty() {
                bail!("entity '{name}' identity has no fields");
            }
            for id_field in identity_names {
                if !entity.fields.contains_key(id_field) {
                    bail!(
                        "entity '{name}' identity field '{id_field}' is not declared under fields"
                    );
                }
            }
            for (rel_name, rel) in &entity.relations {
                if !entity.fields.contains_key(&rel.local) {
                    bail!(
                        "entity '{name}' relation '{rel_name}' local field '{}' missing",
                        rel.local
                    );
                }
                if !self.entities.contains_key(&rel.entity) {
                    bail!(
                        "entity '{name}' relation '{rel_name}' targets unknown entity '{}'",
                        rel.entity
                    );
                }
            }
            for (field_name, field) in &entity.fields {
                if !entity.sources.contains_key(&field.source) {
                    bail!(
                        "entity '{name}' field '{field_name}' references unknown source '{}'",
                        field.source
                    );
                }
                if let Some(column) = &field.column {
                    if column.is_empty() {
                        bail!("entity '{name}' field '{field_name}' has empty column");
                    }
                }
                if let Some(path) = &field.path {
                    if path.is_empty() {
                        bail!("entity '{name}' field '{field_name}' has empty path");
                    }
                }
            }
            for (source_id, source) in &entity.sources {
                match source.source_type.as_str() {
                    SOURCE_POSTGRES => {
                        if source.table.as_ref().map_or(true, |t| t.is_empty()) {
                            bail!(
                                "entity '{name}' postgres source '{source_id}' requires a table name"
                            );
                        }
                    }
                    SOURCE_KAFKA => {
                        if source.topic.as_ref().map_or(true, |t| t.is_empty()) {
                            bail!(
                                "entity '{name}' kafka source '{source_id}' requires a topic"
                            );
                        }
                        if source.entity_key.as_ref().map_or(true, |k| k.is_empty()) {
                            bail!(
                                "entity '{name}' kafka source '{source_id}' requires an entity_key"
                            );
                        }
                    }
                    SOURCE_HTTP => {}
                    SOURCE_MYSQL => {
                        if source.table.as_ref().map_or(true, |t| t.is_empty()) {
                            bail!(
                                "entity '{name}' mysql source '{source_id}' requires a table name"
                            );
                        }
                    }
                    SOURCE_MONGODB => {
                        if source.table.as_ref().map_or(true, |t| t.is_empty()) {
                            bail!(
                                "entity '{name}' mongodb source '{source_id}' requires a collection name in `table`"
                            );
                        }
                    }
                    other => {
                        bail!(
                            "entity '{name}' source '{source_id}' has unsupported type '{other}' \
                             (supported: postgres, kafka, http, mysql, mongodb)"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    pub fn entity(&self, name: &str) -> Option<&EntityDef> {
        self.entities.get(name)
    }

    pub fn has_entity(&self, name: &str) -> bool {
        self.entities.contains_key(name)
    }

    pub fn has_source_type(&self, source_type: &str) -> bool {
        self.entities.values().any(|entity| {
            entity
                .sources
                .values()
                .any(|source| source.source_type == source_type)
        })
    }

    /// Logical entity whose postgres source maps to this physical table, if any.
    pub fn entity_for_table(&self, table: &str) -> Option<(&str, &EntityDef)> {
        self.entity_and_source_for_table(table)
            .map(|(name, _, entity)| (name, entity))
    }

    /// Logical entity + postgres source id for a physical table.
    pub fn entity_and_source_for_table(
        &self,
        table: &str,
    ) -> Option<(&str, &str, &EntityDef)> {
        self.entities.iter().find_map(|(name, entity)| {
            entity.sources.iter().find_map(|(source_id, source)| {
                (source.source_type == SOURCE_POSTGRES && source.table.as_deref() == Some(table))
                    .then_some((name.as_str(), source_id.as_str(), entity))
            })
        })
    }

    /// Postgres table name for a logical entity, if it has a postgres source.
    pub fn postgres_table(&self, entity_name: &str) -> Option<&str> {
        let entity = self.entities.get(entity_name)?;
        entity
            .sources
            .values()
            .find(|s| s.source_type == SOURCE_POSTGRES)
            .and_then(|s| s.table.as_deref())
    }

    /// Distinct physical tables referenced by postgres sources.
    pub fn postgres_tables(&self) -> Vec<String> {
        let mut tables: Vec<String> = self
            .entities
            .values()
            .flat_map(|entity| entity.sources.values())
            .filter(|source| source.source_type == SOURCE_POSTGRES)
            .filter_map(|source| source.table.clone())
            .collect();
        tables.sort();
        tables.dedup();
        tables
    }

    pub fn kafka_bindings(&self) -> Vec<KafkaBinding> {
        let mut bindings = Vec::new();
        for (entity_type, entity) in &self.entities {
            for (source_id, source) in &entity.sources {
                if source.source_type != SOURCE_KAFKA {
                    continue;
                }
                let Some(topic) = source.topic.clone() else {
                    continue;
                };
                let Some(entity_key) = source.entity_key.clone() else {
                    continue;
                };
                bindings.push(KafkaBinding {
                    source_id: source_id.clone(),
                    entity_type: entity_type.clone(),
                    topic,
                    entity_key,
                });
            }
        }
        bindings.sort_by(|a, b| {
            a.entity_type
                .cmp(&b.entity_type)
                .then(a.source_id.cmp(&b.source_id))
        });
        bindings
    }

    /// Field names owned by `source_id` on this entity.
    pub fn fields_owned_by<'a>(&'a self, entity_name: &str, source_id: &str) -> Vec<&'a str> {
        let Some(entity) = self.entities.get(entity_name) else {
            return Vec::new();
        };
        let mut names: Vec<_> = entity
            .fields
            .iter()
            .filter(|(_, f)| f.source == source_id)
            .map(|(name, _)| name.as_str())
            .collect();
        names.sort();
        names
    }

    pub fn field_authority(&self, entity_name: &str, field: &str) -> Option<&str> {
        self.entities
            .get(entity_name)?
            .fields
            .get(field)
            .map(|f| f.source.as_str())
    }

    /// Source id key for the postgres adapter on this entity (e.g. `"postgres"`).
    pub fn postgres_source_id(&self, entity_name: &str) -> Option<&str> {
        let entity = self.entities.get(entity_name)?;
        entity
            .sources
            .iter()
            .find(|(_, s)| s.source_type == SOURCE_POSTGRES)
            .map(|(id, _)| id.as_str())
    }

    /// First `ordering` field declared on fields owned by `source_id`, if any.
    pub fn ordering_for_source(&self, entity_name: &str, source_id: &str) -> Option<&str> {
        let entity = self.entities.get(entity_name)?;
        let mut names: Vec<_> = entity
            .fields
            .iter()
            .filter(|(_, f)| f.source == source_id)
            .filter_map(|(name, f)| f.ordering.as_deref().map(|o| (name.as_str(), o)))
            .collect();
        names.sort_by(|a, b| a.0.cmp(b.0));
        names.first().map(|(_, o)| *o)
    }

    /// Physical Postgres column for a logical field (defaults to the field name).
    pub fn physical_column<'a>(&'a self, entity_name: &str, field: &'a str) -> Option<&'a str> {
        let f = self.entities.get(entity_name)?.fields.get(field)?;
        Some(f.column.as_deref().unwrap_or(field))
    }

    /// Physical JSON path for a logical field (defaults to the field name).
    pub fn physical_path<'a>(&'a self, entity_name: &str, field: &'a str) -> Option<&'a str> {
        let f = self.entities.get(entity_name)?.fields.get(field)?;
        Some(f.path.as_deref().unwrap_or(field))
    }

    /// Map of logical field → physical column for fields owned by `source_id`.
    pub fn column_map(&self, entity_name: &str, source_id: &str) -> HashMap<String, String> {
        let Some(entity) = self.entities.get(entity_name) else {
            return HashMap::new();
        };
        entity
            .fields
            .iter()
            .filter(|(_, f)| f.source == source_id)
            .map(|(name, f)| {
                (
                    name.clone(),
                    f.column.clone().unwrap_or_else(|| name.clone()),
                )
            })
            .collect()
    }

    /// Map of logical field → physical JSON path for fields owned by `source_id`.
    pub fn path_map(&self, entity_name: &str, source_id: &str) -> HashMap<String, String> {
        let Some(entity) = self.entities.get(entity_name) else {
            return HashMap::new();
        };
        entity
            .fields
            .iter()
            .filter(|(_, f)| f.source == source_id)
            .map(|(name, f)| {
                (
                    name.clone(),
                    f.path.clone().unwrap_or_else(|| name.clone()),
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
entities:
  driver:
    identity:
      field: id
    sources:
      postgres:
        type: postgres
        table: drivers
      gps:
        type: kafka
        topic: driver-locations
        entity_key: driver_id
      http:
        type: http
    fields:
      id:
        source: postgres
        mode: transactional
      name:
        source: postgres
        mode: transactional
      status:
        source: postgres
        mode: transactional
      location:
        source: gps
        mode: latest_value
        ordering: sequence
"#;

    #[test]
    fn loads_and_queries_sample() {
        let schema = SyncSchema::from_yaml_str(SAMPLE).unwrap();
        assert!(schema.has_entity("driver"));
        assert_eq!(schema.postgres_table("driver"), Some("drivers"));
        assert_eq!(
            schema.entity_for_table("drivers").map(|(n, _)| n),
            Some("driver")
        );
        assert_eq!(schema.field_authority("driver", "location"), Some("gps"));
        assert_eq!(
            schema.fields_owned_by("driver", "postgres"),
            vec!["id", "name", "status"]
        );
        assert!(schema.has_source_type("kafka"));
        assert_eq!(
            schema.kafka_bindings(),
            vec![KafkaBinding {
                source_id: "gps".into(),
                entity_type: "driver".into(),
                topic: "driver-locations".into(),
                entity_key: "driver_id".into(),
            }]
        );
    }

    #[test]
    fn rejects_unknown_field_source() {
        let bad = r#"
entities:
  driver:
    identity: { field: id }
    sources:
      postgres: { type: postgres, table: drivers }
    fields:
      id: { source: missing }
"#;
        assert!(SyncSchema::from_yaml_str(bad).is_err());
    }

    #[test]
    fn rejects_kafka_without_topic_or_entity_key() {
        let no_topic = r#"
entities:
  driver:
    identity: { field: id }
    sources:
      gps: { type: kafka, entity_key: driver_id }
    fields:
      id: { source: gps }
"#;
        assert!(SyncSchema::from_yaml_str(no_topic).is_err());

        let no_key = r#"
entities:
  driver:
    identity: { field: id }
    sources:
      gps: { type: kafka, topic: driver-locations }
    fields:
      id: { source: gps }
"#;
        assert!(SyncSchema::from_yaml_str(no_key).is_err());
    }

    #[test]
    fn rejects_unknown_source_type() {
        let bad = r#"
entities:
  driver:
    identity: { field: id }
    sources:
      oracle: { type: oracle }
    fields:
      id: { source: oracle }
"#;
        let err = SyncSchema::from_yaml_str(bad).unwrap_err().to_string();
        assert!(err.contains("unsupported type"), "{err}");
    }
}
