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

/// Loss of the live resolver worker's outcome channel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum ResolverWorkerError {
    /// The worker exited or panicked while the host still owned it.
    Lost,
}

impl fmt::Display for ResolverWorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("resolver worker was lost")
    }
}

impl std::error::Error for ResolverWorkerError {}
