//! Handwritten sync schema: logical entities, sources, and field authority.
//!
//! The CDS merge path consults this contract so multiple backends can contribute
//! fields to one entity without wiping each other. Automatic discovery is out of
//! scope; this crate only loads and validates a YAML file.

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct SyncSchema {
    pub entities: HashMap<String, EntityDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EntityDef {
    pub identity: IdentityDef,
    pub sources: HashMap<String, SourceDef>,
    pub fields: HashMap<String, FieldDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IdentityDef {
    pub field: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceDef {
    #[serde(rename = "type")]
    pub source_type: String,
    /// Physical table name when `source_type` is `postgres`.
    #[serde(default)]
    pub table: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FieldDef {
    /// Authoritative source id (key under `sources`).
    pub source: String,
    #[serde(default = "default_mode")]
    pub mode: FieldMode,
    #[serde(default)]
    pub ordering: Option<String>,
    #[serde(default)]
    pub flush_ms: Option<u64>,
}

fn default_mode() -> FieldMode {
    FieldMode::Transactional
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldMode {
    Transactional,
    LatestValue,
}

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
            if !entity.fields.contains_key(&entity.identity.field) {
                bail!(
                    "entity '{name}' identity field '{}' is not declared under fields",
                    entity.identity.field
                );
            }
            for (field_name, field) in &entity.fields {
                if !entity.sources.contains_key(&field.source) {
                    bail!(
                        "entity '{name}' field '{field_name}' references unknown source '{}'",
                        field.source
                    );
                }
            }
            for (source_id, source) in &entity.sources {
                if source.source_type == "postgres" && source.table.as_ref().map_or(true, |t| t.is_empty())
                {
                    bail!(
                        "entity '{name}' postgres source '{source_id}' requires a table name"
                    );
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

    /// Logical entity whose postgres source maps to this physical table, if any.
    pub fn entity_for_table(&self, table: &str) -> Option<(&str, &EntityDef)> {
        self.entities.iter().find_map(|(name, entity)| {
            entity
                .sources
                .values()
                .any(|s| s.source_type == "postgres" && s.table.as_deref() == Some(table))
                .then_some((name.as_str(), entity))
        })
    }

    /// Postgres table name for a logical entity, if it has a postgres source.
    pub fn postgres_table(&self, entity_name: &str) -> Option<&str> {
        let entity = self.entities.get(entity_name)?;
        entity
            .sources
            .values()
            .find(|s| s.source_type == "postgres")
            .and_then(|s| s.table.as_deref())
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
            .find(|(_, s)| s.source_type == "postgres")
            .map(|(id, _)| id.as_str())
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
        source: http
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
        assert_eq!(schema.field_authority("driver", "location"), Some("http"));
        assert_eq!(
            schema.fields_owned_by("driver", "postgres"),
            vec!["id", "name", "status"]
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
}
