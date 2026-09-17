//! Naming and mapping heuristics for schema generation.

use kafka_source::InferredField;
use postgres_source::TableInfo;

/// Naive English singularization for table → entity names.
pub fn singularize(name: &str) -> String {
    let lower = name.to_ascii_lowercase();
    if lower.ends_with("ies") && lower.len() > 3 {
        return format!("{}y", &name[..name.len() - 3]);
    }
    // Avoid stripping endings that look like singular Latin/English forms.
    if lower.ends_with('s')
        && !lower.ends_with("ss")
        && !lower.ends_with("us")
        && !lower.ends_with("is")
        && !lower.ends_with("os")
        && lower.len() > 1
    {
        return name[..name.len() - 1].to_string();
    }
    name.to_string()
}

/// Fields that look like high-frequency latest-value state.
pub fn is_latest_value_field(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "lat"
            | "lng"
            | "lon"
            | "longitude"
            | "latitude"
            | "location"
            | "heading"
            | "speed"
            | "bearing"
            | "altitude"
            | "geo"
            | "coords"
            | "coordinates"
            | "position"
    )
}

/// Fields that look like source-local ordering / sequence numbers.
pub fn is_ordering_field(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "sequence" | "seq" | "offset" | "version" | "event_id" | "revision"
    )
}

/// Prefer `sequence`, then other ordering-like names present on the sample.
pub fn propose_ordering(fields: &[InferredField]) -> Option<String> {
    let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
    if names.iter().any(|n| n.eq_ignore_ascii_case("sequence")) {
        return Some("sequence".to_string());
    }
    names
        .into_iter()
        .find(|n| is_ordering_field(n))
        .map(str::to_string)
}

/// Propose a kafka payload field that identifies the logical entity.
///
/// Prefers `*_id` that matches a known entity PK / name, else any `*_id`, else `id`.
pub fn propose_entity_key(
    fields: &[InferredField],
    entities: &[(String, String)],
) -> Option<String> {
    // entities: (logical_name, identity_field)
    let field_names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();

    for (entity_name, identity) in entities {
        let candidate = format!("{entity_name}_id");
        if field_names
            .iter()
            .any(|n| n.eq_ignore_ascii_case(&candidate))
        {
            return field_names
                .iter()
                .find(|n| n.eq_ignore_ascii_case(&candidate))
                .map(|s| (*s).to_string());
        }
        if identity == "id" {
            // also try table-style: drivers → driver_id already covered by singular entity name
        }
        if field_names
            .iter()
            .any(|n| n.eq_ignore_ascii_case(identity) && *identity != "id")
        {
            return Some(identity.clone());
        }
    }

    if let Some(id_like) = field_names.iter().find(|n| {
        let lower = n.to_ascii_lowercase();
        lower.ends_with("_id") || lower == "id"
    }) {
        return Some((*id_like).to_string());
    }

    None
}

/// Propose which entity a topic should attach to, from topic name heuristics.
pub fn propose_attach_entity(topic: &str, entity_names: &[String]) -> Option<String> {
    let normalized_topic = normalize_token(topic);
    let mut best: Option<(usize, String)> = None;
    for name in entity_names {
        let entity = normalize_token(name);
        let tableish = normalize_token(&format!("{name}s"));
        if normalized_topic.contains(&entity) || normalized_topic.contains(&tableish) {
            let score = entity.len();
            if best.as_ref().map(|(s, _)| score > *s).unwrap_or(true) {
                best = Some((score, name.clone()));
            }
        }
    }
    best.map(|(_, name)| name)
}

/// Source nickname derived from a topic (`driver-locations` → `driver_locations`).
pub fn source_id_from_topic(topic: &str) -> String {
    let mut out = String::new();
    for ch in topic.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if !out.ends_with('_') && !out.is_empty() {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "kafka".to_string()
    } else {
        trimmed.to_string()
    }
}

fn normalize_token(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// Whether a postgres table is eligible for auto entity generation.
pub fn table_is_eligible(table: &TableInfo) -> bool {
    !table.primary_key.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_source::InferredField;

    #[test]
    fn singularize_basic() {
        assert_eq!(singularize("drivers"), "driver");
        assert_eq!(singularize("companies"), "company");
        assert_eq!(singularize("status"), "status");
        assert_eq!(singularize("glass"), "glass");
    }

    #[test]
    fn ordering_prefers_sequence() {
        let fields = vec![
            InferredField {
                name: "seq".into(),
                inferred_type: "number".into(),
            },
            InferredField {
                name: "sequence".into(),
                inferred_type: "number".into(),
            },
        ];
        assert_eq!(propose_ordering(&fields).as_deref(), Some("sequence"));
    }

    #[test]
    fn entity_key_matches_driver() {
        let fields = vec![
            InferredField {
                name: "driver_id".into(),
                inferred_type: "number".into(),
            },
            InferredField {
                name: "location".into(),
                inferred_type: "object".into(),
            },
        ];
        let entities = vec![("driver".into(), "id".into())];
        assert_eq!(
            propose_entity_key(&fields, &entities).as_deref(),
            Some("driver_id")
        );
    }

    #[test]
    fn attach_from_topic_name() {
        let entities = vec!["driver".into(), "job".into()];
        assert_eq!(
            propose_attach_entity("driver-locations", &entities).as_deref(),
            Some("driver")
        );
        assert_eq!(propose_attach_entity("metrics", &entities), None);
    }

    #[test]
    fn source_id_sanitizes_topic() {
        assert_eq!(source_id_from_topic("driver-locations"), "driver_locations");
        assert_eq!(source_id_from_topic("___"), "kafka");
    }
}
