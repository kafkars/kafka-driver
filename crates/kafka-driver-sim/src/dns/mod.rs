//! Scripted broker resolution over shared socket-free endpoint vocabulary.

mod plan;
mod script;

#[cfg(test)]
mod script_test;

pub use kafka_driver_core::{BrokerEndpoint, HostName, HostNameError, IpAddress, ResolvedAddress};
pub use plan::{DnsFailure, DnsOutcome, DnsRequest, DnsStep};
pub use script::{DnsScriptError, ScriptedDns};
