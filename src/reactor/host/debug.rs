//! Sanitized host diagnostics exposing state without endpoint or request material.

use super::Reactor;

impl std::fmt::Debug for Reactor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Reactor")
            .field("limits", &self.limits)
            .field("broker", &self.brokers.seed_broker_state())
            .field("connection", &self.brokers.seed_connection_state())
            .field("advertised_brokers", &self.brokers.advertised_brokers())
            .field("resolving_names", &self.resolution.is_some())
            .field("metadata_generation", &self.brokers.directory_generation())
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}
