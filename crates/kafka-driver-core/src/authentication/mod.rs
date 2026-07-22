//! Deterministic SASL handshake and bounded exchange-round policy.

mod deadline;
mod effect;
mod exchange;
mod failure;
mod handshake;
mod input;
mod limits;
mod machine;
mod mechanism;
mod state;
mod transition;

#[cfg(test)]
mod machine_test;

pub use effect::AuthenticationEffect;
pub use failure::AuthenticationFailure;
pub use input::{AuthenticationAttempt, AuthenticationInput, ExchangeOutcome};
pub use limits::AuthenticationLimits;
pub use machine::AuthenticationMachine;
pub use mechanism::{SaslMechanism, SaslProtocol};
pub use state::{AuthenticationPhase, AuthenticationRound, AuthenticationState};
pub use transition::{AuthenticationDisposition, AuthenticationTransition};

use machine::Decision;
use state::StateData;
