//! Deterministic simulation substrate for kafka-driver machines.
//!
//! The simulator advances virtual time only when consuming a scripted event.
//! It owns no sockets, threads, real clock, or runtime integration.

mod clock;
mod dns;
mod event;
mod limits;
mod plan;
mod schedule;
mod simulation;

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
pub use simulation::{SimulationError, Simulator};
