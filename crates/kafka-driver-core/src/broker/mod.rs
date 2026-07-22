//! Long-lived broker reconnect policy above replaceable connection epochs.

mod effect;
mod input;
mod machine;
mod policy;
mod state;
mod transition;

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod policy_test;

pub use effect::BrokerEffect;
pub use input::{BrokerInput, ReconnectSchedule};
pub use machine::BrokerMachine;
pub use policy::{BackoffPolicy, BackoffPolicyError, JitterSample, RetryOrdinal};
pub use state::{BrokerCloseReason, BrokerPhase, BrokerState};
pub use transition::{BrokerDisposition, BrokerTransition};
