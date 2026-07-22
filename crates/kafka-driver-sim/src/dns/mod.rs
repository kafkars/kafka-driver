//! Scripted broker resolution over shared socket-free endpoint vocabulary.

mod plan;
mod script;

#[cfg(test)]
mod script_test;

pub use kafka_driver_core::{
    BrokerEndpoint, DnsFailure, DnsOutcome, DnsRequest, HostName, HostNameError, IpAddress,
    ResolutionLimits, ResolvedAddress, ResolvedAddressSet, ResolvedAddressSetError,
};
pub use plan::DnsStep;
pub use script::{DnsScriptError, ScriptedDns};
