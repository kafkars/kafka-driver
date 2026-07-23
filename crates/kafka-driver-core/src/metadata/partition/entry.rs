//! One validated topic-partition leader assignment.

use crate::{BrokerId, LeaderEpoch, MetadataRevision, PartitionId, TopicName};

/// Broker ownership for one partition from one accepted topic observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionLeader {
    topic: TopicName,
    partition: PartitionId,
    broker_id: BrokerId,
    leader_epoch: Option<LeaderEpoch>,
    revision: MetadataRevision,
}

impl PartitionLeader {
    /// Creates one assignment from already validated Kafka identities.
    pub const fn new(
        topic: TopicName,
        partition: PartitionId,
        broker_id: BrokerId,
        leader_epoch: Option<LeaderEpoch>,
        revision: MetadataRevision,
    ) -> Self {
        Self {
            topic,
            partition,
            broker_id,
            leader_epoch,
            revision,
        }
    }

    /// Returns the topic owned by this assignment.
    pub const fn topic(&self) -> &TopicName {
        &self.topic
    }

    /// Returns the partition index within the topic.
    pub const fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Returns the broker currently leading the partition.
    pub const fn broker_id(&self) -> BrokerId {
        self.broker_id
    }

    /// Returns the broker-issued leader epoch when the negotiated API supplied one.
    pub const fn leader_epoch(&self) -> Option<LeaderEpoch> {
        self.leader_epoch
    }

    /// Returns the accepted Metadata operation that observed this leader fact.
    pub const fn revision(&self) -> MetadataRevision {
        self.revision
    }
}
