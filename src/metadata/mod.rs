//! Validation bridge from generated Metadata responses into driver-owned facts.

mod broker_snapshot;
mod error;

#[cfg(test)]
mod broker_snapshot_test;

pub(crate) use broker_snapshot::broker_snapshot_from_response;
pub(crate) use error::MetadataBuildError;
