//! Bounded conversion of generated topic partitions into known leader facts.

use std::num::NonZeroU32;

use kafka_driver_core::{
    BrokerId, EvidenceStamp, KafkaTopicId, LeaderEpoch, MetadataRevision, PartitionId,
    PartitionLeader, PartitionLeaderLimits, PartitionLeaderSet, TopicName, TopicPartitionCount,
};
use kafka_wire::{
    MetadataResponse,
    metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
};

use super::MetadataBuildError;

#[derive(Debug, Eq, PartialEq)]
pub(super) struct TopicPartitionSnapshot {
    pub(super) leaders: PartitionLeaderSet,
    pub(super) count: Option<TopicPartitionCount>,
}

pub(super) fn partition_facts_for_topic(
    response: &MetadataResponse,
    expected: &TopicName,
    revision: MetadataRevision,
    evidence: EvidenceStamp,
    limits: PartitionLeaderLimits,
) -> Result<TopicPartitionSnapshot, MetadataBuildError> {
    enforce_input_bounds(response, limits)?;
    let [topic] = response.topics.as_slice() else {
        return Err(MetadataBuildError::TopicResponseCount {
            observed: response.topics.len(),
        });
    };
    let name = topic
        .name
        .as_ref()
        .ok_or(MetadataBuildError::TopicNameMissing)?;
    let name = TopicName::new(name.as_str()).map_err(MetadataBuildError::TopicName)?;
    if &name != expected {
        return Err(MetadataBuildError::RequestedTopicMismatch);
    }
    let (leaders, count) = if topic.error_code == 0 {
        validate_logical_range(topic)?;
        let count = u32::try_from(topic.partitions.len())
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or(MetadataBuildError::TopicPartitionsEmpty)?;
        let topic_id = KafkaTopicId::from_bytes(topic.topic_id.to_bytes());
        let count = match topic_id {
            Some(topic_id) => TopicPartitionCount::new_with_id(name, topic_id, count),
            None => TopicPartitionCount::new(name, count),
        };
        (
            topic_leaders(topic, count.topic(), revision, evidence)?,
            Some(count),
        )
    } else {
        (Vec::new(), None)
    };
    let leaders = PartitionLeaderSet::try_from_iter(leaders, limits)
        .map_err(MetadataBuildError::PartitionLeaders)?;
    Ok(TopicPartitionSnapshot { leaders, count })
}

fn enforce_input_bounds(
    response: &MetadataResponse,
    limits: PartitionLeaderLimits,
) -> Result<(), MetadataBuildError> {
    let topic_limit = limits.max_topics().get();
    if response.topics.len() > topic_limit {
        return Err(MetadataBuildError::TopicCapacity {
            observed: response.topics.len(),
            limit: topic_limit,
        });
    }
    let partition_limit = limits.max_partitions().get();
    let observed = response
        .topics
        .iter()
        .try_fold(0usize, |count, topic| {
            count.checked_add(topic.partitions.len())
        })
        .unwrap_or(usize::MAX);
    if observed > partition_limit {
        return Err(MetadataBuildError::PartitionCapacity {
            observed,
            limit: partition_limit,
        });
    }
    Ok(())
}

fn topic_leaders(
    topic: &MetadataResponseTopic,
    name: &TopicName,
    revision: MetadataRevision,
    evidence: EvidenceStamp,
) -> Result<Vec<PartitionLeader>, MetadataBuildError> {
    let mut leaders = Vec::with_capacity(topic.partitions.len().min(16));
    for partition in topic
        .partitions
        .iter()
        .filter(|partition| partition.error_code == 0)
    {
        if let Some(leader) = partition_leader(name.clone(), partition, revision, evidence)? {
            leaders.push(leader);
        }
    }
    Ok(leaders)
}

fn validate_logical_range(topic: &MetadataResponseTopic) -> Result<(), MetadataBuildError> {
    if topic.partitions.is_empty() {
        return Err(MetadataBuildError::TopicPartitionsEmpty);
    }
    let mut partitions = Vec::with_capacity(topic.partitions.len());
    for partition in &topic.partitions {
        partitions.push(
            PartitionId::new(partition.partition_index).map_err(MetadataBuildError::PartitionId)?,
        );
    }
    partitions.sort_unstable();
    if let Some(duplicate) = partitions.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(MetadataBuildError::DuplicateTopicPartition {
            partition: duplicate[0],
        });
    }
    for (expected, actual) in partitions.into_iter().enumerate() {
        let actual_index = usize::try_from(actual.get())
            .map_err(|_| MetadataBuildError::PartitionIndexOverflow { partition: actual })?;
        if actual_index != expected {
            return Err(MetadataBuildError::TopicPartitionMissing {
                expected,
                next: actual,
            });
        }
    }
    Ok(())
}

fn partition_leader(
    topic: TopicName,
    partition: &MetadataResponsePartition,
    revision: MetadataRevision,
    evidence: EvidenceStamp,
) -> Result<Option<PartitionLeader>, MetadataBuildError> {
    let partition_id =
        PartitionId::new(partition.partition_index).map_err(MetadataBuildError::PartitionId)?;
    let leader_epoch = if partition.leader_epoch == -1 {
        None
    } else {
        Some(LeaderEpoch::new(partition.leader_epoch).map_err(|source| {
            MetadataBuildError::LeaderEpoch {
                partition: partition_id,
                source,
            }
        })?)
    };
    if partition.leader_id == -1 {
        return Ok(None);
    }
    let broker_id =
        BrokerId::new(partition.leader_id).map_err(|source| MetadataBuildError::LeaderId {
            partition: partition_id,
            source,
        })?;
    Ok(Some(PartitionLeader::new_with_evidence(
        topic,
        partition_id,
        broker_id,
        leader_epoch,
        revision,
        evidence,
    )))
}
