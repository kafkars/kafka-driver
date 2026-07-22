//! Public bounds for retained broker membership and internal Metadata RPC waits.

use std::time::Duration;

use kafka_driver_core::BrokerDirectoryLimits;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Resource and wait bounds applied to cluster metadata refreshes.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataLimits {
    broker_directory: BrokerDirectoryLimits,
    request_timeout: Duration,
}

impl MetadataLimits {
    /// Creates explicit broker-retention and Metadata RPC bounds.
    pub const fn new(broker_directory: BrokerDirectoryLimits, request_timeout: Duration) -> Self {
        Self {
            broker_directory,
            request_timeout,
        }
    }

    /// Returns maximum broker membership retained in one generation.
    pub const fn broker_directory(self) -> BrokerDirectoryLimits {
        self.broker_directory
    }

    /// Returns the maximum wait assigned to one generated Metadata RPC.
    pub const fn request_timeout(self) -> Duration {
        self.request_timeout
    }

    pub(super) const fn default_limits() -> Self {
        Self::new(BrokerDirectoryLimits::defaults(), DEFAULT_REQUEST_TIMEOUT)
    }
}

impl Default for MetadataLimits {
    fn default() -> Self {
        Self::default_limits()
    }
}
