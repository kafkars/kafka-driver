//! Single-owner construction of one bounded public operational snapshot.

use crate::{BootstrapSnapshot, DriverSnapshot};

use super::Reactor;

impl Reactor {
    pub(super) fn snapshot(&self) -> DriverSnapshot {
        let observation = self.observation.snapshot();
        let bootstrap = BootstrapSnapshot::new(
            self.resolution
                .as_ref()
                .and_then(super::NameResolution::last_bootstrap_dns_failure),
            self.brokers.seed_snapshot(),
        );
        DriverSnapshot::new(
            self.commands.snapshot(),
            self.brokers.directory_generation(),
            bootstrap,
            self.brokers.lane_snapshots(),
            observation.calls,
            observation.failures,
            observation.latency,
        )
    }
}
