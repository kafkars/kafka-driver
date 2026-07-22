//! Ownership-preserving resolver request admission failures.

use std::fmt;

use kafka_driver_core::DnsRequest;

/// Why a DNS request did not enter the bounded blocking worker queue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum ResolverSubmitError {
    Full(DnsRequest),
    Closed(DnsRequest),
}

impl fmt::Display for ResolverSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => formatter.write_str("resolver request capacity reached"),
            Self::Closed(_) => formatter.write_str("resolver worker is closed"),
        }
    }
}

impl std::error::Error for ResolverSubmitError {}
