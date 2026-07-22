//! Scripted broker resolution and logical address vocabulary.

mod address;
mod plan;
mod script;

#[cfg(test)]
mod address_test;
#[cfg(test)]
mod script_test;

pub use address::{BrokerEndpoint, HostName, HostNameError, IpAddress, ResolvedAddress};
pub use plan::{DnsFailure, DnsOutcome, DnsRequest, DnsStep};
pub use script::{DnsScriptError, ScriptedDns};
