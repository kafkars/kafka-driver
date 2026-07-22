//! Validation bridge from generated Metadata responses into driver-owned facts.

mod broker_snapshot;
mod error;
mod partition_snapshot;
mod snapshot;

#[cfg(test)]
mod broker_snapshot_test;
#[cfg(test)]
mod partition_snapshot_test;

pub(crate) use error::MetadataBuildError;
pub(crate) use snapshot::snapshot_from_response;
