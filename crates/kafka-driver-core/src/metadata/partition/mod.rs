//! Bounded partition-leader facts and generation-fenced route vocabulary.

mod entry;
mod error;
mod limits;
mod route;
mod set;

#[cfg(test)]
mod set_test;

pub use entry::PartitionLeader;
pub use error::PartitionLeaderSetError;
pub use limits::PartitionLeaderLimits;
pub use route::PartitionRoute;
pub use set::PartitionLeaderSet;
