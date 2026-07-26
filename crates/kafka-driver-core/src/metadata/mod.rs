//! Immutable cluster metadata generations and semantic route ownership.

mod admission;
mod decision;
mod effect;
mod error;
mod input;
mod machine;
mod outcome;
mod partition;
mod query;
mod revocation;
mod snapshot;
mod state;
mod topic;
mod transition;

#[cfg(test)]
mod causality_test;
#[cfg(test)]
mod epoch_test;
#[cfg(test)]
mod invalidation_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod query_test;
#[cfg(test)]
mod snapshot_test;
#[cfg(test)]
mod topic_test;

pub use effect::MetadataEffect;
pub use error::MetadataSnapshotError;
pub use input::MetadataInput;
pub use machine::MetadataMachine;
pub use partition::{
    PartitionLeader, PartitionLeaderLimits, PartitionLeaderSet, PartitionLeaderSetError,
    PartitionRoute,
};
pub use query::{MetadataQuery, MetadataQueryLimits};
pub use snapshot::MetadataSnapshot;
pub use state::MetadataState;
pub use topic::{TopicPartitionCount, TopicPartitionCountSet, TopicPartitionCountSetError};
pub use transition::{MetadataDisposition, MetadataTransition};
