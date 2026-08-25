//! Driver-independent Kafka metadata observation for movement qualification.

use std::{error::Error, fmt, net::SocketAddr};

use kafka_wire::MetadataResponse;

use crate::{endpoint, error::ProbeError};

mod wire;

pub(super) fn run(
    bootstrap: &str,
    topic: &str,
    broker_id: i32,
    advertised: Option<&str>,
) -> Result<(), ProbeError> {
    let bootstrap = endpoint::socket(bootstrap)
        .map_err(|source| ProbeError::stage("validate metadata bootstrap address", source))?;
    let advertised = advertised
        .map(endpoint::socket)
        .transpose()
        .map_err(|source| ProbeError::stage("validate advertised broker address", source))?;
    let response = wire::fetch(bootstrap, topic)?;
    let (label, summary) = match advertised {
        Some(advertised) => (
            "authoritative movement metadata",
            validate(&response, topic, broker_id, advertised),
        ),
        None => (
            "authoritative broker fencing",
            validate_fenced(&response, topic, broker_id),
        ),
    };
    let summary = summary.map_err(|source| ProbeError::stage(label, source))?;
    println!("PASS {label}: {summary}");
    Ok(())
}

pub(super) fn validate(
    response: &MetadataResponse,
    topic: &str,
    broker_id: i32,
    advertised: SocketAddr,
) -> Result<String, MetadataMismatch> {
    let summary = summary(response, topic);
    let broker = response
        .brokers
        .iter()
        .find(|broker| broker.node_id == broker_id)
        .ok_or_else(|| mismatch("expected broker is absent", &summary))?;
    if broker.host.as_str() != advertised.ip().to_string()
        || broker.port != i32::from(advertised.port())
    {
        return Err(mismatch(
            "expected broker advertisement is absent",
            &summary,
        ));
    }
    let topic = response
        .topics
        .iter()
        .find(|metadata| {
            metadata
                .name
                .as_ref()
                .is_some_and(|name| name.as_str() == topic)
        })
        .ok_or_else(|| mismatch("expected topic is absent", &summary))?;
    if topic.error_code != 0 {
        return Err(mismatch("topic metadata has an error", &summary));
    }
    let partition = topic
        .partitions
        .iter()
        .find(|partition| partition.partition_index == 0)
        .ok_or_else(|| mismatch("partition zero is absent", &summary))?;
    if partition.error_code != 0 {
        return Err(mismatch("partition metadata has an error", &summary));
    }
    if partition.leader_id != broker_id {
        return Err(mismatch("partition leader has not moved back", &summary));
    }
    if !partition.isr_nodes.contains(&broker_id) {
        return Err(mismatch("partition leader is not in sync", &summary));
    }
    Ok(summary)
}

pub(super) fn validate_fenced(
    response: &MetadataResponse,
    topic: &str,
    broker_id: i32,
) -> Result<String, MetadataMismatch> {
    let summary = summary(response, topic);
    if response
        .brokers
        .iter()
        .any(|broker| broker.node_id == broker_id)
    {
        return Err(mismatch("stopped broker remains registered", &summary));
    }
    let topic = response
        .topics
        .iter()
        .find(|metadata| {
            metadata
                .name
                .as_ref()
                .is_some_and(|name| name.as_str() == topic)
        })
        .ok_or_else(|| mismatch("expected topic is absent", &summary))?;
    if topic.error_code != 0 {
        return Err(mismatch("topic metadata has an error", &summary));
    }
    let partition = topic
        .partitions
        .iter()
        .find(|partition| partition.partition_index == 0)
        .ok_or_else(|| mismatch("partition zero is absent", &summary))?;
    if partition.leader_id == broker_id || partition.isr_nodes.contains(&broker_id) {
        return Err(mismatch("stopped broker remains the live leader", &summary));
    }
    Ok(summary)
}

fn summary(response: &MetadataResponse, expected_topic: &str) -> String {
    let brokers = response
        .brokers
        .iter()
        .map(|broker| format!("{}={}:{}", broker.node_id, broker.host, broker.port))
        .collect::<Vec<_>>()
        .join(",");
    let partition = response
        .topics
        .iter()
        .find(|topic| {
            topic
                .name
                .as_ref()
                .is_some_and(|name| name.as_str() == expected_topic)
        })
        .and_then(|topic| {
            topic
                .partitions
                .iter()
                .find(|partition| partition.partition_index == 0)
        })
        .map_or_else(
            || "partition=absent".to_owned(),
            |partition| {
                format!(
                    "partition=0,error={},leader={},isr={:?}",
                    partition.error_code, partition.leader_id, partition.isr_nodes
                )
            },
        );
    format!("brokers=[{brokers}],{partition}")
}

fn mismatch(reason: &str, summary: &str) -> MetadataMismatch {
    MetadataMismatch(format!("{reason}; observed {summary}"))
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct MetadataMismatch(String);

impl fmt::Display for MetadataMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for MetadataMismatch {}
