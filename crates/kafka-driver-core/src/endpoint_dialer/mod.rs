//! Deterministic candidate rotation and re-resolution policy for one endpoint.

mod effect;
mod input;
mod machine;
mod transition;

#[cfg(test)]
mod machine_test;

pub use effect::EndpointDialerEffect;
pub use input::EndpointDialerInput;
pub use machine::EndpointDialer;
pub use transition::EndpointDialerTransition;
