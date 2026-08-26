//! Single-broker ownership above deterministic policy and below the host.

#[cfg(test)]
mod address_refresh;
#[cfg(test)]
mod authentication;
#[cfg(test)]
mod construction;
mod deadline;
#[cfg(test)]
mod error;
#[cfg(test)]
mod failure;
#[cfg(test)]
mod identity;
mod limits;
#[cfg(test)]
mod local_rejection;
#[cfg(test)]
mod negotiation;
#[cfg(test)]
mod observation;
#[cfg(test)]
mod open;
#[cfg(test)]
mod owner;
#[cfg(test)]
mod readiness;
#[cfg(test)]
mod reconnect;
#[cfg(test)]
mod replacement;
#[cfg(test)]
mod response;
#[cfg(test)]
mod shutdown;
#[cfg(test)]
mod simulation;
#[cfg(test)]
mod submission;
#[cfg(test)]
mod terminal;
#[cfg(test)]
mod write_admission;

#[cfg(test)]
mod address_refresh_terminal_test;
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
#[cfg(test)]
pub(in crate::reactor) use error::BrokerError;
#[cfg(test)]
pub(in crate::reactor) use identity::BrokerIds;
pub(in crate::reactor) use limits::BrokerLimits;
#[cfg(test)]
pub(in crate::reactor) use owner::SingleBroker;
