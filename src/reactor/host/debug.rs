//! Sanitized host diagnostics exposing state without endpoint or request material.

use super::Reactor;

impl std::fmt::Debug for Reactor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let legacy = self.backend.legacy();
        let broker = legacy.and_then(|legacy| legacy.brokers.seed_broker_state());
        let connection = legacy.and_then(|legacy| legacy.brokers.seed_connection_state());
        let advertised_brokers = legacy.map_or(0, |legacy| legacy.brokers.advertised_brokers());
        let allocated_lanes = legacy.map_or(0, |legacy| legacy.brokers.allocated_lanes());
        let connected_lanes = legacy.map_or(0, |legacy| legacy.brokers.connected_lanes());
        let resolving_lanes = legacy.map_or(0, |legacy| legacy.brokers.resolving_lanes());
        let waiting_calls = legacy.map_or(0, |legacy| legacy.brokers.waiting_calls());
        let metadata_generation = legacy.and_then(|legacy| legacy.brokers.directory_generation());
        formatter
            .debug_struct("Reactor")
            .field("limits", &self.limits)
            .field("broker", &broker)
            .field("connection", &connection)
            .field("advertised_brokers", &advertised_brokers)
            .field("allocated_lanes", &allocated_lanes)
            .field("connected_lanes", &connected_lanes)
            .field("resolving_lanes", &resolving_lanes)
            .field("waiting_calls", &waiting_calls)
            .field("resolving_names", &self.resolution.is_some())
            .field("metadata_generation", &metadata_generation)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}
