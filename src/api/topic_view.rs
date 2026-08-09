//! Public immutable projection of one driver-owned exact-topic metadata generation.

use std::{fmt, num::NonZeroUsize, time::Instant};

use kafka_driver_core::{
    KafkaTopicId, LeaderEpoch, MetadataGeneration, MetadataSnapshot, PartitionId, TopicName,
};

use crate::{completion::completion_pair, reactor::Command};

use super::{Call, Driver, SubmitError};

/// One currently routable partition in canonical partition-index order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AvailableTopicPartition {
    partition: PartitionId,
    broker_id: kafka_driver_core::BrokerId,
    leader_epoch: Option<LeaderEpoch>,
}

impl AvailableTopicPartition {
    /// Returns the available logical partition.
    pub const fn partition(self) -> PartitionId {
        self.partition
    }

    /// Returns the broker currently leading this partition.
    pub const fn broker_id(self) -> kafka_driver_core::BrokerId {
        self.broker_id
    }

    /// Returns the broker-issued leader epoch when Metadata supplied one.
    pub const fn leader_epoch(self) -> Option<LeaderEpoch> {
        self.leader_epoch
    }
}

/// Bounded immutable topic facts copied from one installed driver snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicView {
    topic: TopicName,
    topic_id: Option<KafkaTopicId>,
    generation: MetadataGeneration,
    logical_partition_count: u32,
    available: Vec<AvailableTopicPartition>,
}

impl TopicView {
    /// Borrows the exact topic identity completed by this view.
    pub const fn topic(&self) -> &TopicName {
        &self.topic
    }

    /// Returns the broker-issued topic identity when the negotiated Metadata version supplied one.
    pub const fn topic_id(&self) -> Option<KafkaTopicId> {
        self.topic_id
    }

    /// Returns the installed immutable cluster generation.
    pub const fn generation(&self) -> MetadataGeneration {
        self.generation
    }

    /// Returns the total logical count, including partitions without known leaders.
    pub const fn logical_partition_count(&self) -> u32 {
        self.logical_partition_count
    }

    /// Returns the number of partitions with a currently known leader.
    pub fn available_len(&self) -> usize {
        self.available.len()
    }

    /// Borrows one available fact by canonical partition-order index.
    pub fn available_at(&self, index: usize) -> Option<&AvailableTopicPartition> {
        self.available.get(index)
    }

    pub(crate) fn from_snapshot(
        snapshot: &MetadataSnapshot,
        topic: &TopicName,
    ) -> Result<Option<Self>, TopicViewError> {
        let Some(count) = snapshot.topic_partition_counts().find(topic) else {
            return Ok(None);
        };
        let available_len = snapshot
            .partition_leaders()
            .iter()
            .filter(|leader| leader.topic() == topic)
            .count();
        let mut available = Vec::new();
        available
            .try_reserve_exact(available_len)
            .map_err(|_| TopicViewError::ProjectionAllocationFailed)?;
        for leader in snapshot
            .partition_leaders()
            .iter()
            .filter(|leader| leader.topic() == topic)
        {
            if u32::try_from(leader.partition().get())
                .map_or(true, |partition| partition >= count.count().get())
            {
                return Err(TopicViewError::MalformedMetadata);
            }
            available.push(AvailableTopicPartition {
                partition: leader.partition(),
                broker_id: leader.broker_id(),
                leader_epoch: leader.leader_epoch(),
            });
        }
        Ok(Some(Self {
            topic: topic.clone(),
            topic_id: count.topic_id(),
            generation: snapshot.generation(),
            logical_partition_count: count.count().get(),
            available,
        }))
    }

    pub(crate) const fn maximum_available_bytes(max_partitions: NonZeroUsize) -> usize {
        size_of::<AvailableTopicPartition>().saturating_mul(max_partitions.get())
    }
}

/// Why an admitted topic-view request could not return installed topic facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TopicViewError {
    /// The caller-owned absolute deadline elapsed.
    DeadlineExceeded,
    /// Exact-topic metadata completed without usable facts.
    Unavailable,
    /// The driver's owned Metadata refresh or its completion channel failed.
    RefreshFailed,
    /// Kafka rejected the exact topic lookup with its signed protocol code.
    Broker {
        /// Exact broker-issued Metadata error code.
        error_code: i16,
    },
    /// A successful Metadata exchange did not describe a coherent logical topic.
    MalformedMetadata,
    /// Bounded view projection could not reserve its exact available-entry storage.
    ProjectionAllocationFailed,
    /// Distinct metadata refresh demand reached its configured bound.
    QueryCapacityReached {
        /// Maximum queued distinct metadata queries.
        limit: usize,
    },
    /// Retained topic-view wait ownership reached its count or byte bound.
    CapacityReached {
        /// Maximum retained view waiters.
        call_limit: usize,
        /// Maximum bytes retained by view waiters.
        byte_limit: usize,
    },
    /// Graceful shutdown won before the command was interpreted.
    Draining,
}

impl fmt::Display for TopicViewError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineExceeded => formatter.write_str("topic-view deadline exceeded"),
            Self::Unavailable => formatter.write_str("topic metadata is unavailable"),
            Self::RefreshFailed => formatter.write_str("topic metadata refresh failed"),
            Self::Broker { error_code } => {
                write!(
                    formatter,
                    "topic metadata failed with Kafka error {error_code}"
                )
            }
            Self::MalformedMetadata => formatter.write_str("topic metadata response is malformed"),
            Self::ProjectionAllocationFailed => {
                formatter.write_str("topic-view projection allocation failed")
            }
            Self::QueryCapacityReached { limit } => {
                write!(formatter, "metadata query capacity {limit} is full")
            }
            Self::CapacityReached {
                call_limit,
                byte_limit,
            } => write!(
                formatter,
                "topic-view capacity is full at {call_limit} calls or {byte_limit} bytes"
            ),
            Self::Draining => formatter.write_str("the driver is draining"),
        }
    }
}

impl std::error::Error for TopicViewError {}

impl Driver {
    /// Requests one exact-topic immutable view under the caller's original deadline.
    pub fn topic_view(
        &self,
        topic: TopicName,
        deadline: Instant,
    ) -> Result<Call<Result<TopicView, TopicViewError>>, SubmitError> {
        let (completion, sender) = completion_pair();
        self.commands
            .try_send(Command::TopicView {
                topic,
                deadline,
                result_capacity_bytes: self.topic_view_result_capacity_bytes,
                completion: sender,
            })
            .map_err(SubmitError::from)?;
        Ok(Call::new(completion))
    }
}
