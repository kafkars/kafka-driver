//! Sanitized failures while validating generated cluster membership.

use std::{error::Error, fmt};

use kafka_driver_core::{
    BrokerDirectoryError, BrokerId, BrokerIdError, HostNameError, LeaderEpochError,
    MetadataSnapshotError, PartitionId, PartitionIdError, PartitionLeaderSetError, TopicNameError,
    TopicPartitionCountSetError,
};

/// Why a generated Metadata response could not become immutable driver facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MetadataBuildError {
    Response {
        error_code: i16,
    },
    BrokerCapacity {
        observed: usize,
        limit: usize,
    },
    TopicCapacity {
        observed: usize,
        limit: usize,
    },
    PartitionCapacity {
        observed: usize,
        limit: usize,
    },
    TopicResponseCount {
        observed: usize,
    },
    BrokerId(BrokerIdError),
    BrokerHost {
        broker_id: BrokerId,
        source: HostNameError,
    },
    BrokerPort {
        broker_id: BrokerId,
        port: i32,
    },
    Directory(BrokerDirectoryError),
    ControllerId(BrokerIdError),
    TopicNameMissing,
    RequestedTopicMismatch,
    TopicName(TopicNameError),
    TopicPartitionsEmpty,
    DuplicateTopicPartition {
        partition: PartitionId,
    },
    TopicPartitionMissing {
        expected: usize,
        next: PartitionId,
    },
    PartitionIndexOverflow {
        partition: PartitionId,
    },
    PartitionId(PartitionIdError),
    LeaderId {
        partition: PartitionId,
        source: BrokerIdError,
    },
    LeaderEpoch {
        partition: PartitionId,
        source: LeaderEpochError,
    },
    PartitionLeaders(PartitionLeaderSetError),
    TopicCounts(TopicPartitionCountSetError),
    Snapshot(MetadataSnapshotError),
}

impl fmt::Display for MetadataBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Response { error_code } => {
                write!(
                    formatter,
                    "Metadata response failed with Kafka error {error_code}"
                )
            }
            Self::BrokerCapacity { observed, limit } => write!(
                formatter,
                "Metadata response advertises {observed} brokers, limit is {limit}"
            ),
            Self::TopicCapacity { observed, limit } => write!(
                formatter,
                "Metadata response advertises {observed} topics, limit is {limit}"
            ),
            Self::PartitionCapacity { observed, limit } => write!(
                formatter,
                "Metadata response advertises {observed} partitions, limit is {limit}"
            ),
            Self::TopicResponseCount { observed } => write!(
                formatter,
                "single-topic Metadata response contains {observed} topics"
            ),
            Self::BrokerId(source) => write!(formatter, "invalid metadata broker: {source}"),
            Self::BrokerHost { broker_id, source } => write!(
                formatter,
                "invalid host advertised for broker {}: {source}",
                broker_id.get()
            ),
            Self::BrokerPort { broker_id, port } => write!(
                formatter,
                "invalid port {port} advertised for broker {}",
                broker_id.get()
            ),
            Self::Directory(source) => write!(formatter, "invalid broker membership: {source}"),
            Self::ControllerId(source) => {
                write!(formatter, "invalid metadata controller: {source}")
            }
            Self::TopicNameMissing => formatter.write_str("successful metadata topic has no name"),
            Self::RequestedTopicMismatch => {
                formatter.write_str("Metadata response does not match the requested topic")
            }
            Self::TopicName(source) => write!(formatter, "invalid metadata topic name: {source}"),
            Self::TopicPartitionsEmpty => {
                formatter.write_str("successful metadata topic has no logical partitions")
            }
            Self::DuplicateTopicPartition { partition } => write!(
                formatter,
                "metadata topic repeats partition {}",
                partition.get()
            ),
            Self::TopicPartitionMissing { expected, next } => write!(
                formatter,
                "metadata topic is missing partition {expected} before {}",
                next.get()
            ),
            Self::PartitionIndexOverflow { partition } => write!(
                formatter,
                "metadata partition {} exceeds the local index domain",
                partition.get()
            ),
            Self::PartitionId(source) => {
                write!(formatter, "invalid metadata partition: {source}")
            }
            Self::LeaderId { partition, source } => write!(
                formatter,
                "invalid leader for partition {}: {source}",
                partition.get()
            ),
            Self::LeaderEpoch { partition, source } => write!(
                formatter,
                "invalid leader epoch for partition {}: {source}",
                partition.get()
            ),
            Self::PartitionLeaders(source) => {
                write!(formatter, "invalid partition leader index: {source}")
            }
            Self::TopicCounts(source) => {
                write!(formatter, "invalid topic partition-count facts: {source}")
            }
            Self::Snapshot(source) => write!(formatter, "incoherent cluster metadata: {source}"),
        }
    }
}

impl Error for MetadataBuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::BrokerId(source) | Self::ControllerId(source) | Self::LeaderId { source, .. } => {
                Some(source)
            }
            Self::BrokerHost { source, .. } => Some(source),
            Self::Directory(source) => Some(source),
            Self::TopicName(source) => Some(source),
            Self::PartitionId(source) => Some(source),
            Self::LeaderEpoch { source, .. } => Some(source),
            Self::PartitionLeaders(source) => Some(source),
            Self::TopicCounts(source) => Some(source),
            Self::Snapshot(source) => Some(source),
            Self::Response { .. }
            | Self::BrokerCapacity { .. }
            | Self::TopicCapacity { .. }
            | Self::PartitionCapacity { .. }
            | Self::TopicResponseCount { .. }
            | Self::BrokerPort { .. }
            | Self::TopicNameMissing
            | Self::RequestedTopicMismatch
            | Self::TopicPartitionsEmpty
            | Self::DuplicateTopicPartition { .. }
            | Self::TopicPartitionMissing { .. }
            | Self::PartitionIndexOverflow { .. } => None,
        }
    }
}
