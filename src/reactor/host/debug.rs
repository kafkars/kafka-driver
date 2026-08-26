//! Sanitized host diagnostics exposing state without endpoint or request material.

use super::Reactor;

impl std::fmt::Debug for Reactor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cluster = self.backend.cluster();
        let seed = cluster
            .and_then(super::super::direct_plaintext::ClusterBackend::seed_snapshot)
            .or_else(|| {
                self.backend
                    .direct()
                    .and_then(super::super::direct_plaintext::DirectBackend::seed_snapshot)
            });
        let broker = seed.map(crate::SeedSnapshot::broker_state);
        let connection = seed.map(crate::SeedSnapshot::connection_phase);
        let advertised_brokers = cluster.map_or(
            0,
            super::super::direct_plaintext::ClusterBackend::advertised_brokers,
        );
        let allocated_lanes = cluster.map_or(
            0,
            super::super::direct_plaintext::ClusterBackend::allocated_lanes,
        );
        let connected_lanes = 0;
        let resolving_lanes = 0;
        let waiting_calls = 0;
        let metadata_generation =
            cluster.and_then(super::super::direct_plaintext::ClusterBackend::directory_generation);
        let mut diagnostics = formatter.debug_struct("Reactor");
        diagnostics
            .field("limits", &self.limits)
            .field("broker", &broker);
        diagnostics.field("connection", &connection);
        diagnostics
            .field("advertised_brokers", &advertised_brokers)
            .field("allocated_lanes", &allocated_lanes)
            .field("connected_lanes", &connected_lanes)
            .field("resolving_lanes", &resolving_lanes)
            .field("waiting_calls", &waiting_calls)
            .field("resolving_names", &self.resolution.is_some())
            .field("metadata_generation", &metadata_generation)
            .field("state", &self.state);
        diagnostics.finish_non_exhaustive()
    }
}
