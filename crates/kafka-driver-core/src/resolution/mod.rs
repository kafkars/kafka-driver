//! Bounded data exchanged between deterministic owners and DNS interpreters.

mod address_set;
mod error;
mod limits;
mod outcome;
mod request;

#[cfg(test)]
mod address_set_test;

pub use address_set::ResolvedAddressSet;
pub use error::ResolvedAddressSetError;
pub use limits::ResolutionLimits;
pub use outcome::{DnsFailure, DnsOutcome};
pub use request::DnsRequest;
