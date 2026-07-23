//! Semantic cluster destinations independent of sockets and connection lanes.

use kafka_driver_core::{CoordinatorKey, PartitionId, TopicName};

/// Kafka ownership fact required before a generated request can be submitted.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Route {
    /// Uses the currently available seed connection without metadata lookup.
    AnyBroker,

    /// Uses the controller broker from the current immutable metadata generation.
    Controller,

    /// Uses the broker currently coordinating one group, transaction, or share key.
    Coordinator {
        /// Validated key and coordinator namespace.
        key: CoordinatorKey,
    },

    /// Uses the current leader for one exact topic partition.
    PartitionLeader {
        /// Validated topic whose leader owns this call.
        topic: TopicName,

        /// Nonnegative partition index within the topic.
        partition: PartitionId,
    },
}

impl Route {
    pub(crate) fn heap_bytes(&self) -> usize {
        match self {
            Self::AnyBroker | Self::Controller => 0,
            Self::Coordinator { key } => key.heap_bytes(),
            Self::PartitionLeader { topic, .. } => topic.heap_bytes(),
        }
    }
}
