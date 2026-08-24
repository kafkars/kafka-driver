//! Single-broker ownership above deterministic policy and below the host.

mod address_refresh;
mod address_rotation;
mod authentication;
mod construction;
mod deadline;
mod error;
mod failure;
mod identity;
mod limits;
mod local_rejection;
mod negotiation;
mod observation;
mod open;
mod owner;
mod readiness;
mod reconnect;
mod replacement;
mod response;
mod shutdown;
#[cfg(test)]
mod simulation;
mod submission;
mod terminal;
mod write_admission;

#[cfg(test)]
mod address_refresh_terminal_test;
#[cfg(test)]
mod address_rotation_test;
#[cfg(test)]
pub(in crate::reactor) mod authentication_fixture_test;
#[cfg(test)]
mod authentication_test;
#[cfg(test)]
mod deadline_test;
#[cfg(test)]
mod identity_test;
#[cfg(test)]
mod interest_failure_test;
#[cfg(test)]
mod lifecycle_test;
#[cfg(test)]
mod limits_test;
#[cfg(test)]
mod negotiation_test;
#[cfg(test)]
mod read_failure_test;
#[cfg(test)]
mod reconnect_test;
#[cfg(test)]
mod replacement_test;
#[cfg(test)]
mod round_trip_test;
#[cfg(test)]
pub(super) mod scenario_support_test;
#[cfg(test)]
mod scram_authentication_failure_test;
#[cfg(test)]
mod scram_authentication_test;
#[cfg(test)]
mod scram_start_error_test;
#[cfg(test)]
mod submission_test;

pub(in crate::reactor) use deadline::DeadlineProgress;
pub(in crate::reactor) use error::BrokerError;
pub(in crate::reactor) use identity::BrokerIds;
pub(in crate::reactor) use limits::BrokerLimits;
pub(in crate::reactor) use owner::SingleBroker;
