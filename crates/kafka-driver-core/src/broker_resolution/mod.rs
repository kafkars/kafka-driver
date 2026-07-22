//! Deterministic advertised-endpoint DNS ownership for one broker identity.

mod effect;
mod input;
mod machine;
mod state;
mod transition;

#[cfg(test)]
mod machine_test;

pub use effect::BrokerResolutionEffect;
pub use input::BrokerResolutionInput;
pub use machine::BrokerResolutionMachine;
pub use state::BrokerResolutionState;
pub use transition::{BrokerResolutionDisposition, BrokerResolutionTransition};
