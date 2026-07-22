//! Single-owner construction of one bounded public operational snapshot.

use crate::DriverSnapshot;

use super::Reactor;

impl Reactor {
    pub(super) fn snapshot(&self) -> DriverSnapshot {
        let observation = self.observation.snapshot();
        DriverSnapshot::new(
            self.commands.snapshot(),
            self.brokers.directory_generation(),
            self.brokers.seed_snapshot(),
            self.brokers.lane_snapshots(),
            observation.calls,
            observation.failures,
            observation.latency,
        )
    }
}
