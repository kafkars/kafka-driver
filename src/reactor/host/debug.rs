//! Sanitized host diagnostics exposing state without endpoint or request material.

use super::Reactor;

impl std::fmt::Debug for Reactor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let legacy = self.backend.legacy();
        let cluster = self.backend.cluster();
        let cluster_seed =
            cluster.and_then(super::super::direct_plaintext::ClusterBackend::seed_snapshot);
        let broker = cluster_seed
            .map(crate::SeedSnapshot::broker_state)
            .or_else(|| legacy.and_then(|legacy| legacy.brokers.seed_broker_state()));
        let connection = cluster_seed.map(crate::SeedSnapshot::connection_phase);
        let legacy_connection = legacy.and_then(|legacy| legacy.brokers.seed_connection_state());
        let advertised_brokers = cluster.map_or_else(
            || legacy.map_or(0, |legacy| legacy.brokers.advertised_brokers()),
            super::super::direct_plaintext::ClusterBackend::advertised_brokers,
        );
        let allocated_lanes = cluster.map_or_else(
            || legacy.map_or(0, |legacy| legacy.brokers.allocated_lanes()),
            super::super::direct_plaintext::ClusterBackend::allocated_lanes,
        );
        let connected_lanes = legacy.map_or(0, |legacy| legacy.brokers.connected_lanes());
        let resolving_lanes = legacy.map_or(0, |legacy| legacy.brokers.resolving_lanes());
        let waiting_calls = legacy.map_or(0, |legacy| legacy.brokers.waiting_calls());
        let metadata_generation = cluster
            .and_then(super::super::direct_plaintext::ClusterBackend::directory_generation)
            .or_else(|| legacy.and_then(|legacy| legacy.brokers.directory_generation()));
        let mut diagnostics = formatter.debug_struct("Reactor");
        diagnostics
            .field("limits", &self.limits)
            .field("broker", &broker);
        if cluster.is_some() {
            diagnostics.field("connection", &connection);
        } else {
            diagnostics.field("connection", &legacy_connection);
        }
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
