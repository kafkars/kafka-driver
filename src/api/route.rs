//! Semantic cluster destinations independent of sockets and connection lanes.

/// Kafka ownership fact required before a generated request can be submitted.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Route {
    /// Uses the currently available seed connection without metadata lookup.
    AnyBroker,

    /// Uses the controller broker from the current immutable metadata generation.
    Controller,
}
