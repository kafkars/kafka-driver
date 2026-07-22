//! Immutable cluster metadata generations and semantic route ownership.

mod error;
mod snapshot;

#[cfg(test)]
mod snapshot_test;

pub use error::MetadataSnapshotError;
pub use snapshot::MetadataSnapshot;
