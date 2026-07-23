//! Long-lived broker reconnect policy above replaceable connection epochs.

mod effect;
mod endpoint_refresh;
mod input;
mod machine;
mod policy;
mod recovery;
mod state;
mod transition;

#[cfg(test)]
mod endpoint_refresh_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod policy_test;
#[cfg(test)]
mod recovery_test;

pub use effect::BrokerEffect;
pub use input::{BrokerInput, EndpointRefreshSchedule, ReconnectSchedule};
pub use machine::BrokerMachine;
pub use policy::{BackoffPolicy, BackoffPolicyError, JitterSample, RetryOrdinal};
pub use state::{AddressRefreshState, BrokerCloseReason, BrokerPhase, BrokerState};
pub use transition::{BrokerDisposition, BrokerTransition};
