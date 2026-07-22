//! Partition ownership fenced by metadata generation and broker-issued leader epoch.

use crate::{BrokerRoute, LeaderEpoch, PartitionId, TopicName};

/// Permission to route one call using one immutable leader assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionRoute {
    broker: BrokerRoute,
    topic: TopicName,
    partition: PartitionId,
    leader_epoch: Option<LeaderEpoch>,
}

impl PartitionRoute {
    pub(in crate::metadata) const fn new(
        broker: BrokerRoute,
        topic: TopicName,
        partition: PartitionId,
        leader_epoch: Option<LeaderEpoch>,
    ) -> Self {
        Self {
            broker,
            topic,
            partition,
            leader_epoch,
        }
    }

    /// Returns the exact metadata-generation broker route.
    pub const fn broker_route(&self) -> BrokerRoute {
        self.broker
    }

    /// Returns the topic whose partition authorized this route.
    pub const fn topic(&self) -> &TopicName {
        &self.topic
    }

    /// Returns the routed partition index.
    pub const fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Returns the known leader epoch, if supplied by the negotiated Metadata API.
    pub const fn leader_epoch(&self) -> Option<LeaderEpoch> {
        self.leader_epoch
    }
}
