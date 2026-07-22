//! Single-broker ownership above deterministic policy and below the host.

mod authentication;
mod construction;
mod deadline;
mod entropy;
mod error;
mod failure;
mod identity;
mod limits;
mod negotiation;
mod owner;
mod readiness;
mod reconnect;
mod replacement;
mod response;
mod shutdown;
mod submission;
mod terminal;

#[cfg(test)]
mod authentication_fixture_test;
#[cfg(test)]
mod authentication_test;
#[cfg(test)]
mod deadline_test;
#[cfg(test)]
mod entropy_test;
#[cfg(test)]
mod identity_test;
#[cfg(test)]
mod lifecycle_test;
#[cfg(test)]
mod limits_test;
#[cfg(test)]
mod negotiation_test;
#[cfg(test)]
mod reconnect_test;
#[cfg(test)]
mod replacement_test;
#[cfg(test)]
mod round_trip_test;
#[cfg(test)]
mod scenario_support_test;
#[cfg(test)]
mod scram_authentication_test;
#[cfg(test)]
mod submission_test;

pub(in crate::reactor) use deadline::DeadlineProgress;
pub(in crate::reactor) use error::BrokerError;
pub(in crate::reactor) use identity::BrokerIds;
pub(in crate::reactor) use limits::BrokerLimits;
pub(in crate::reactor) use owner::SingleBroker;
