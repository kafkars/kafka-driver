//! Sanitized host diagnostics exposing state without endpoint or request material.

use crate::reactor::metadata::MetadataOwner;

use super::Reactor;

impl std::fmt::Debug for Reactor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Reactor")
            .field("limits", &self.limits)
            .field("broker", &self.brokers.seed_broker_state())
            .field("connection", &self.brokers.seed_connection_state())
            .field("resolving_names", &self.resolution.is_some())
            .field(
                "metadata_generation",
                &self.metadata.as_ref().and_then(MetadataOwner::generation),
            )
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}
