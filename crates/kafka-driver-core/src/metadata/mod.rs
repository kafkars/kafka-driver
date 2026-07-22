//! Immutable cluster metadata generations and semantic route ownership.

mod effect;
mod error;
mod input;
mod machine;
mod partition;
mod snapshot;
mod state;
mod transition;

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod snapshot_test;

pub use effect::MetadataEffect;
pub use error::MetadataSnapshotError;
pub use input::MetadataInput;
pub use machine::MetadataMachine;
pub use partition::{
    PartitionLeader, PartitionLeaderLimits, PartitionLeaderSet, PartitionLeaderSetError,
    PartitionRoute,
};
pub use snapshot::MetadataSnapshot;
pub use state::MetadataState;
pub use transition::{MetadataDisposition, MetadataTransition};
