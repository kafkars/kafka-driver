//! Bounded per-connection API capabilities selected during negotiation.

mod api;
mod error;
mod set;

#[cfg(test)]
mod set_test;

pub use api::NegotiatedApi;
pub use error::CapabilityError;
pub use set::NegotiatedCapabilities;
