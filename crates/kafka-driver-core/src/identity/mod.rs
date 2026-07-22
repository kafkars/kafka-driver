//! Typed identities that make stale external work rejectable as data.

mod broker;
mod call;
mod coordinator;
mod effect;
mod partition;
mod topic;
mod transport;

#[cfg(test)]
mod broker_test;
#[cfg(test)]
mod coordinator_test;
#[cfg(test)]
mod partition_test;
#[cfg(test)]
mod topic_test;

pub use broker::{BrokerId, BrokerIdError, MetadataGeneration};
pub use call::{CallId, OperationId};
pub use coordinator::{CoordinatorEpoch, CoordinatorKey, CoordinatorKeyError, CoordinatorKind};
pub use effect::{EffectId, TimerId};
pub use partition::{LeaderEpoch, LeaderEpochError, PartitionId, PartitionIdError};
pub use topic::{TopicName, TopicNameError};
pub use transport::{ConnectionEpoch, TransportId};
