//! Kafka capability scripts shared by deterministic driver simulations.
//!
//! Calandria owns scheduling and virtual time; this crate retains only Kafka-
//! shaped DNS, readiness, and byte-stream fixtures.

mod dns;
mod poller;
mod transport;

#[cfg(test)]
mod scenario;

#[cfg(test)]
mod authentication_scenario_test;
#[cfg(test)]
mod bootstrap_scenario_test;
#[cfg(test)]
mod broker_resolution_scenario_test;
#[cfg(test)]
mod broker_scenario_test;
#[cfg(test)]
mod connection_scenario_test;

pub use calandria_sim::Planned;
pub use dns::{
    BrokerEndpoint, DnsFailure, DnsOutcome, DnsRequest, DnsScriptError, DnsStep, HostName,
    HostNameError, IpAddress, ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
    ResolvedAddressSetError, ScriptedDns,
};
pub use poller::{
    PollInterest, PollRequest, PollScriptError, PollStep, Readiness, ReadinessEvent, ScriptedPoller,
};
#[cfg(test)]
pub(crate) use scenario::Scenario;
pub use transport::{
    FaultPlan, ReadRequest, ReadResult, ReadStep, ScriptedTransport, TransportFault,
    TransportIdentity, TransportOperationKind, TransportOutcome, TransportPlanError,
    TransportScriptError, TransportStep, WriteRequest, WriteResult, WriteStep,
};
