//! Deterministic simulation substrate for kafka-driver machines.
//!
//! The simulator advances virtual time only when consuming a scripted event.
//! It owns no sockets, threads, real clock, or runtime integration.

mod clock;
mod dns;
mod event;
mod limits;
mod plan;
mod poller;
mod schedule;
mod simulation;
mod transport;

#[cfg(test)]
mod authentication_scenario_test;
#[cfg(test)]
mod broker_scenario_test;
#[cfg(test)]
mod clock_test;
#[cfg(test)]
mod connection_scenario_test;
#[cfg(test)]
mod schedule_test;
#[cfg(test)]
mod simulation_test;

pub use clock::{ClockError, SimClock};
pub use dns::{
    BrokerEndpoint, DnsFailure, DnsOutcome, DnsRequest, DnsScriptError, DnsStep, HostName,
    HostNameError, IpAddress, ResolvedAddress, ScriptedDns,
};
pub use event::{Scheduled, SimEventId};
pub use limits::SimulationLimits;
pub use plan::Planned;
pub use poller::{
    PollInterest, PollRequest, PollScriptError, PollStep, Readiness, ReadinessEvent, ScriptedPoller,
};
pub use simulation::{SimulationError, Simulator};
pub use transport::{
    FaultPlan, ReadRequest, ReadResult, ReadStep, ScriptedTransport, TransportFault,
    TransportIdentity, TransportOperationKind, TransportOutcome, TransportPlanError,
    TransportScriptError, TransportStep, WriteRequest, WriteResult, WriteStep,
};
