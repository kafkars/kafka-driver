//! Bootstrap resolution and installed seed state without endpoint disclosure.

use kafka_driver_core::DnsFailure;

use super::SeedSnapshot;

/// Current bootstrap resolution diagnostic and installed seed ownership.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BootstrapSnapshot {
    last_dns_failure: Option<DnsFailure>,
    seed: Option<SeedSnapshot>,
}

impl BootstrapSnapshot {
    pub(crate) const fn new(
        last_dns_failure: Option<DnsFailure>,
        seed: Option<SeedSnapshot>,
    ) -> Self {
        Self {
            last_dns_failure,
            seed,
        }
    }

    /// Returns the last sanitized bootstrap-name resolution failure.
    pub const fn last_dns_failure(self) -> Option<DnsFailure> {
        self.last_dns_failure
    }

    /// Borrows the installed seed connection when bootstrap has succeeded.
    pub const fn seed(&self) -> Option<&SeedSnapshot> {
        self.seed.as_ref()
    }
}
