//! Bounded generational ownership of reactor I/O resources.

mod error;
mod identity;
mod registry;

#[cfg(test)]
mod registry_test;

pub(in crate::reactor) use error::{ResourceAdmissionError, ResourceAdmissionFailure};
pub(in crate::reactor) use identity::{ResourceIdentity, ResourceToken};
