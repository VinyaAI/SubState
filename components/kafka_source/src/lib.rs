//! Kafka follow: JSON messages → [`SourceUpdate`].
//!
//! Brokers come from process env. Topic and entity key come from the sync schema.

pub mod discover;

use anyhow::{Context, Result};
use cds::SourceUpdate;
use futures_util::StreamExt;
use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::ClientBuilder;
use schema::{KafkaBinding, SyncSchema};
use serde_json::{Map, Value};
use source::{Follow, SourceEvent};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

pub use discover::{
    discover_topics, infer_fields_from_samples, is_internal_topic, InferredField, TopicSample,
    DEFAULT_SAMPLE_SIZE,
};

pub const CONSUMER_GROUP: &str = "substate";

pub struct KafkaFollow {
    pub brokers: Vec<String>,
    pub sync_schema: Arc<SyncSchema>,
}

impl Follow for KafkaFollow {
    fn spawn(self: Box<Self>, tx: mpsc::Sender<SourceEvent>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let bindings = self.sync_schema.kafka_bindings();
            if bindings.is_empty() {
                return;
            }
            info!(
                brokers = self.brokers.join(","),
                topics = bindings.len(),
                group = CONSUMER_GROUP,
                "kafka follow started"
            );
            let mut tasks = Vec::new();
            for binding in bindings {
                let tx = tx.clone();
                let brokers = self.brokers.clone();
                let schema = Arc::clone(&self.sync_schema);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = consume_binding(&brokers, binding, schema, tx).await {
                        error!(error = %err, "kafka consumer exited");
                    }
                }));
            }
            for task in tasks {
                let _ = task.await;
            }
        })
    }
}

async fn consume_binding(
    brokers: &[String],
    binding: KafkaBinding,
    sync_schema: Arc<SyncSchema>,
    tx: mpsc::Sender<SourceEvent>,
) -> Result<()> {
    let client = ClientBuilder::new(brokers.to_vec())
        .build()
        .await
        .context("kafka client")?;

    let topics = client.list_topics().await.context("list kafka topics")?;
    let partitions: Vec<i32> = topics
        .iter()
        .find(|t| t.name == binding.topic)
        .map(|t| t.partitions.iter().copied().collect())
        .unwrap_or_else(|| vec![0]);

    info!(
        topic = %binding.topic,
        entity = %binding.entity_type,
        partitions = partitions.len(),
        "consuming kafka topic"
    );

    let mut tasks = Vec::new();
    for partition in partitions {
        let tx = tx.clone();
        let brokers = brokers.to_vec();
        let binding = binding.clone();
        let schema = Arc::clone(&sync_schema);
        tasks.push(tokio::spawn(async move {
            if let Err(err) =
                consume_partition(&brokers, binding, schema, partition, tx).await
            {
                error!(error = %err, partition, "kafka partition consumer exited");
            }
        }));
    }
    for task in tasks {
        let _ = task.await;
    }
    Ok(())
}

async fn consume_partition(
    brokers: &[String],
    binding: KafkaBinding,
    sync_schema: Arc<SyncSchema>,
    partition: i32,
    tx: mpsc::Sender<SourceEvent>,
) -> Result<()> {
    let client = ClientBuilder::new(brokers.to_vec())
        .build()
        .await
        .context("kafka client")?;

    let partition_client = client
        .partition_client(
            binding.topic.clone(),
            partition,
            UnknownTopicHandling::Retry,
        )
        .await
        .with_context(|| {
            format!(
                "kafka partition client for {} partition {}",
                binding.topic, partition
            )
        })?;

    let offset_key = format!("{}:{}", binding.topic, partition);
    let start = load_committed_offset(&offset_key)
        .map(StartOffset::At)
        .unwrap_or(StartOffset::Latest);

    let mut stream = StreamConsumerBuilder::new(Arc::new(partition_client), start)
        .with_max_wait_ms(500)
        .build();

    let ordering = sync_schema
        .ordering_for_source(&binding.entity_type, &binding.source_id)
        .map(str::to_string);

    while let Some(item) = stream.next().await {
        let (record, _high_watermark) = match item {
            Ok(pair) => pair,
            Err(err) => {
                warn!(
                    error = %err,
                    topic = %binding.topic,
                    partition,
                    "kafka fetch failed"
                );
                continue;
            }
        };
        let offset = record.offset;
        let Some(bytes) = record.record.value else {
            continue;
        };
        let payload: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(err) => {
                warn!(error = %err, topic = %binding.topic, "kafka payload is not JSON");
                continue;
            }
        };
        let owned = sync_schema.fields_owned_by(&binding.entity_type, &binding.source_id);
        let path_map = sync_schema.path_map(&binding.entity_type, &binding.source_id);
        let Some(update) = project_kafka_json(
            &binding.source_id,
            &binding.entity_type,
            &binding.entity_key,
            &owned,
            &payload,
            offset,
            ordering.as_deref(),
            &path_map,
        ) else {
            warn!(topic = %binding.topic, "kafka message missing entity key");
            continue;
        };
        if tx.send(SourceEvent::Upsert(update)).await.is_err() {
            return Ok(());
        }
        // Commit next offset to read (offset + 1).
        commit_offset(&offset_key, offset + 1);
    }
    Ok(())
}

fn offsets_path() -> std::path::PathBuf {
    std::env::var("SUBSTATE_KAFKA_OFFSETS_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("./substate-kafka-offsets.json"))
}

fn load_committed_offset(key: &str) -> Option<i64> {
    let path = offsets_path();
    let text = std::fs::read_to_string(path).ok()?;
    let map: HashMap<String, i64> = serde_json::from_str(&text).ok()?;
    map.get(key).copied()
}

fn commit_offset(key: &str, next_offset: i64) {
    let path = offsets_path();
    let mut map: HashMap<String, i64> = std::fs::read_to_string(&path)
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default();
    map.insert(key.to_string(), next_offset);
    if let Ok(text) = serde_json::to_string_pretty(&map) {
        let _ = std::fs::write(path, text);
    }
}

/// Map a JSON payload onto a [`SourceUpdate`].
///
/// `path_map` maps logical field → physical JSON key. Missing entries use the
/// logical name.
///
/// Version comes from `ordering_field` when present on the payload, otherwise
/// from `sequence`, otherwise the Kafka offset.
pub fn project_kafka_json(
    source_id: &str,
    entity_type: &str,
    entity_key: &str,
    owned_fields: &[&str],
    payload: &Value,
    offset: i64,
    ordering_field: Option<&str>,
    path_map: &HashMap<String, String>,
) -> Option<SourceUpdate> {
    let object = payload.as_object()?;
    let id = json_id(object.get(entity_key)?)?;
    let version = ordering_field
        .and_then(|name| object.get(name))
        .and_then(json_u64)
        .or_else(|| object.get("sequence").and_then(json_u64))
        .unwrap_or(offset as u64);

    let owned: std::collections::HashSet<&str> = owned_fields.iter().copied().collect();
    let mut fields = Map::new();
    for key in &owned {
        let physical = path_map.get(*key).map(String::as_str).unwrap_or(*key);
        if let Some(value) = object.get(physical).or_else(|| object.get(*key)) {
            fields.insert((*key).to_string(), value.clone());
        }
    }
    if fields.is_empty() {
        return None;
    }

    let versions: HashMap<String, u64> = fields.keys().cloned().map(|k| (k, version)).collect();

    Some(SourceUpdate {
        source: source_id.to_string(),
        entity_type: entity_type.to_string(),
        id,
        fields,
        versions,
    })
}

fn json_id(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

fn json_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64().or_else(|| number.as_i64().map(|n| n as u64)),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::project_kafka_json;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn maps_owned_fields_and_sequence() {
        let owned = ["location"];
        let update = project_kafka_json(
            "gps",
            "driver",
            "driver_id",
            &owned,
            &json!({
                "driver_id": 1,
                "location": {"lat": 36.16, "lng": -86.78},
                "sequence": 42,
                "noise": true
            }),
            99,
            Some("sequence"),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(update.id, "1");
        assert_eq!(update.source, "gps");
        assert_eq!(update.fields["location"], json!({"lat": 36.16, "lng": -86.78}));
        assert!(!update.fields.contains_key("noise"));
        assert_eq!(update.versions.get("location"), Some(&42));
    }

    #[test]
    fn falls_back_to_offset_without_sequence() {
        let owned = ["location"];
        let update = project_kafka_json(
            "gps",
            "driver",
            "driver_id",
            &owned,
            &json!({"driver_id": "728", "location": {"lat": 1.0}}),
            17,
            None,
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(update.id, "728");
        assert_eq!(update.versions.get("location"), Some(&17));
    }

    #[test]
    fn uses_custom_ordering_field() {
        let owned = ["location"];
        let update = project_kafka_json(
            "gps",
            "driver",
            "driver_id",
            &owned,
            &json!({
                "driver_id": 1,
                "location": {"lat": 1.0},
                "seq": 7,
                "sequence": 99
            }),
            1,
            Some("seq"),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(update.versions.get("location"), Some(&7));
    }

    #[test]
    fn remaps_physical_json_path() {
        let owned = ["location"];
        let mut path_map = HashMap::new();
        path_map.insert("location".into(), "coords".into());
        let update = project_kafka_json(
            "gps",
            "driver",
            "driver_id",
            &owned,
            &json!({
                "driver_id": 1,
                "coords": {"lat": 1.0},
                "sequence": 3
            }),
            1,
            Some("sequence"),
            &path_map,
        )
        .unwrap();
        assert_eq!(update.fields["location"], json!({"lat": 1.0}));
        assert!(!update.fields.contains_key("coords"));
    }

    #[test]
    fn skips_missing_entity_key() {
        let owned = ["location"];
        assert!(project_kafka_json(
            "gps",
            "driver",
            "driver_id",
            &owned,
            &json!({"location": {"lat": 1.0}}),
            1,
            None,
            &HashMap::new(),
        )
        .is_none());
    }
}
