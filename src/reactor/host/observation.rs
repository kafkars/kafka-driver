//! Single-owner construction of one bounded public operational snapshot.

use crate::{BootstrapSnapshot, DriverSnapshot};

use super::Reactor;

impl Reactor {
    pub(super) fn snapshot(&self) -> DriverSnapshot {
        let observation = self.observation.snapshot();
        let cluster = self.backend.cluster();
        let seed = cluster
            .and_then(super::super::direct_plaintext::ClusterBackend::seed_snapshot)
            .or_else(|| {
                self.backend
                    .direct()
                    .and_then(super::super::direct_plaintext::DirectBackend::seed_snapshot)
            });
        #[cfg(test)]
        let legacy = self.backend.legacy();
        #[cfg(test)]
        let seed = seed.or_else(|| legacy.and_then(|legacy| legacy.brokers.seed_snapshot()));
        let bootstrap = BootstrapSnapshot::new(
            self.resolution
                .as_ref()
                .and_then(super::NameResolution::last_bootstrap_dns_failure),
            seed,
        );
        let directory_generation =
            cluster.and_then(super::super::direct_plaintext::ClusterBackend::directory_generation);
        #[cfg(test)]
        let directory_generation = directory_generation
            .or_else(|| legacy.and_then(|legacy| legacy.brokers.directory_generation()));
        let lanes = cluster.map_or_else(
            Vec::new,
            super::super::direct_plaintext::ClusterBackend::lane_snapshots,
        );
        #[cfg(test)]
        let lanes = if cluster.is_some() {
            lanes
        } else {
            legacy.map_or_else(Vec::new, |legacy| legacy.brokers.lane_snapshots())
        };
        DriverSnapshot::new(
            self.commands.snapshot(),
            directory_generation,
            bootstrap,
            lanes,
            observation.calls,
            observation.failures,
            observation.latency,
        )
    }
}
