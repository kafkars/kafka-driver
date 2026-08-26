//! Sanitized host diagnostics exposing state without endpoint or request material.

use super::Reactor;

impl std::fmt::Debug for Reactor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cluster = self.backend.cluster();
        #[cfg(test)]
        let legacy = self.backend.legacy();
        let cluster_seed =
            cluster.and_then(super::super::direct_plaintext::ClusterBackend::seed_snapshot);
        let broker = cluster_seed.map(crate::SeedSnapshot::broker_state);
        #[cfg(test)]
        let broker =
            broker.or_else(|| legacy.and_then(|legacy| legacy.brokers.seed_broker_state()));
        let connection = cluster_seed.map(crate::SeedSnapshot::connection_phase);
        #[cfg(test)]
        let legacy_connection = legacy.and_then(|legacy| legacy.brokers.seed_connection_state());
        let advertised_brokers = cluster.map_or(
            0,
            super::super::direct_plaintext::ClusterBackend::advertised_brokers,
        );
        #[cfg(test)]
        let advertised_brokers = if cluster.is_some() {
            advertised_brokers
        } else {
            legacy.map_or(0, |legacy| legacy.brokers.advertised_brokers())
        };
        let allocated_lanes = cluster.map_or(
            0,
            super::super::direct_plaintext::ClusterBackend::allocated_lanes,
        );
        #[cfg(test)]
        let allocated_lanes = if cluster.is_some() {
            allocated_lanes
        } else {
            legacy.map_or(0, |legacy| legacy.brokers.allocated_lanes())
        };
        #[cfg(test)]
        let connected_lanes = legacy.map_or(0, |legacy| legacy.brokers.connected_lanes());
        #[cfg(not(test))]
        let connected_lanes = 0;
        #[cfg(test)]
        let resolving_lanes = legacy.map_or(0, |legacy| legacy.brokers.resolving_lanes());
        #[cfg(not(test))]
        let resolving_lanes = 0;
        #[cfg(test)]
        let waiting_calls = legacy.map_or(0, |legacy| legacy.brokers.waiting_calls());
        #[cfg(not(test))]
        let waiting_calls = 0;
        let metadata_generation =
            cluster.and_then(super::super::direct_plaintext::ClusterBackend::directory_generation);
        #[cfg(test)]
        let metadata_generation = metadata_generation
            .or_else(|| legacy.and_then(|legacy| legacy.brokers.directory_generation()));
        let mut diagnostics = formatter.debug_struct("Reactor");
        diagnostics
            .field("limits", &self.limits)
            .field("broker", &broker);
        if cluster.is_some() {
            diagnostics.field("connection", &connection);
        } else {
            #[cfg(test)]
            diagnostics.field("connection", &legacy_connection);
            #[cfg(not(test))]
            diagnostics.field("connection", &None::<()>);
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
