//! Sanitized host diagnostics exposing state without endpoint or request material.

use crate::reactor::broker::SingleBroker;

use super::Reactor;

impl std::fmt::Debug for Reactor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Reactor")
            .field("limits", &self.limits)
            .field(
                "broker",
                &self.broker.as_ref().map(SingleBroker::broker_state),
            )
            .field("connection", &self.broker.as_ref().map(SingleBroker::state))
            .field("resolving_names", &self.resolution.is_some())
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}
