//! Kafka topic listing and JSON payload sampling for schema generation.

use anyhow::{Context, Result};
use rskafka::client::partition::{OffsetAt, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use tracing::warn;

/// Default number of recent messages to sample per topic.
pub const DEFAULT_SAMPLE_SIZE: usize = 20;

/// One inferred field from sampled JSON payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InferredField {
    pub name: String,
    /// Rough JSON type label: string, number, boolean, object, array, null, mixed.
    pub inferred_type: String,
}

/// Sampled shape of one Kafka topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSample {
    pub topic: String,
    pub fields: Vec<InferredField>,
    pub sample_count: usize,
    pub non_json_count: usize,
}

/// List non-internal topics and sample recent JSON messages on partition 0.
pub async fn discover_topics(
    brokers: &[String],
    sample_size: usize,
) -> Result<Vec<TopicSample>> {
    let client = ClientBuilder::new(brokers.to_vec())
        .build()
        .await
        .context("kafka client for discovery")?;

    let mut topics = client.list_topics().await.context("list kafka topics")?;
    topics.sort_by(|a, b| a.name.cmp(&b.name));

    let mut samples = Vec::new();
    for topic in topics {
        if is_internal_topic(&topic.name) {
            continue;
        }
        match sample_topic(&client, &topic.name, sample_size).await {
            Ok(sample) => samples.push(sample),
            Err(err) => {
                warn!(topic = %topic.name, error = %err, "kafka topic sample failed");
                samples.push(TopicSample {
                    topic: topic.name,
                    fields: Vec::new(),
                    sample_count: 0,
                    non_json_count: 0,
                });
            }
        }
    }
    Ok(samples)
}

/// True for Kafka internal / system topics we should not propose for sync.
pub fn is_internal_topic(name: &str) -> bool {
    name.starts_with("__") || name.starts_with("_schemas") || name == "schemas"
}

async fn sample_topic(
    client: &rskafka::client::Client,
    topic: &str,
    sample_size: usize,
) -> Result<TopicSample> {
    let partition = client
        .partition_client(topic.to_string(), 0, UnknownTopicHandling::Error)
        .await
        .with_context(|| format!("partition client for {topic}"))?;

    let earliest = partition.get_offset(OffsetAt::Earliest).await.unwrap_or(0);
    let latest = partition.get_offset(OffsetAt::Latest).await.unwrap_or(0);

    if latest <= earliest || sample_size == 0 {
        return Ok(TopicSample {
            topic: topic.to_string(),
            fields: Vec::new(),
            sample_count: 0,
            non_json_count: 0,
        });
    }

    let available = (latest - earliest) as usize;
    let take = sample_size.min(available);
    let start = latest - take as i64;

    let (records, _hw) = partition
        .fetch_records(start, 1..1_000_000, 1_000)
        .await
        .with_context(|| format!("fetch samples from {topic}"))?;

    let mut type_votes: BTreeMap<String, BTreeSet<&'static str>> = BTreeMap::new();
    let mut sample_count = 0usize;
    let mut non_json_count = 0usize;

    for record in records.into_iter().take(take) {
        let Some(bytes) = record.record.value else {
            continue;
        };
        match serde_json::from_slice::<Value>(&bytes) {
            Ok(Value::Object(map)) => {
                sample_count += 1;
                for (key, value) in map {
                    type_votes
                        .entry(key)
                        .or_default()
                        .insert(json_type_label(&value));
                }
            }
            Ok(_) => {
                sample_count += 1;
            }
            Err(_) => {
                non_json_count += 1;
            }
        }
    }

    let fields = type_votes
        .into_iter()
        .map(|(name, types)| {
            let inferred_type = if types.len() == 1 {
                types.into_iter().next().unwrap().to_string()
            } else {
                "mixed".to_string()
            };
            InferredField {
                name,
                inferred_type,
            }
        })
        .collect();

    Ok(TopicSample {
        topic: topic.to_string(),
        fields,
        sample_count,
        non_json_count,
    })
}

fn json_type_label(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Infer a field-union from in-memory JSON objects (unit-test helper).
pub fn infer_fields_from_samples(samples: &[Value]) -> Vec<InferredField> {
    let mut type_votes: BTreeMap<String, BTreeSet<&'static str>> = BTreeMap::new();
    for sample in samples {
        let Some(map) = sample.as_object() else {
            continue;
        };
        for (key, value) in map {
            type_votes
                .entry(key.clone())
                .or_default()
                .insert(json_type_label(value));
        }
    }
    type_votes
        .into_iter()
        .map(|(name, types)| {
            let inferred_type = if types.len() == 1 {
                types.into_iter().next().unwrap().to_string()
            } else {
                "mixed".to_string()
            };
            InferredField {
                name,
                inferred_type,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn skips_internal_topics() {
        assert!(is_internal_topic("__consumer_offsets"));
        assert!(is_internal_topic("_schemas"));
        assert!(!is_internal_topic("driver-locations"));
    }

    #[test]
    fn infers_union_types() {
        let fields = infer_fields_from_samples(&[
            json!({"driver_id": 1, "location": {"lat": 1.0}, "sequence": 1}),
            json!({"driver_id": "2", "status": "ok", "sequence": 2}),
        ]);
        let by_name: BTreeMap<_, _> = fields
            .iter()
            .map(|f| (f.name.as_str(), f.inferred_type.as_str()))
            .collect();
        assert_eq!(by_name["driver_id"], "mixed");
        assert_eq!(by_name["location"], "object");
        assert_eq!(by_name["sequence"], "number");
        assert_eq!(by_name["status"], "string");
    }
}
