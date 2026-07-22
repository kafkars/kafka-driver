//! Semantic cluster destinations independent of sockets and connection lanes.

use kafka_driver_core::{PartitionId, TopicName};

/// Kafka ownership fact required before a generated request can be submitted.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Route {
    /// Uses the currently available seed connection without metadata lookup.
    AnyBroker,

    /// Uses the controller broker from the current immutable metadata generation.
    Controller,

    /// Uses the current leader for one exact topic partition.
    PartitionLeader {
        /// Validated topic whose leader owns this call.
        topic: TopicName,

        /// Nonnegative partition index within the topic.
        partition: PartitionId,
    },
}
