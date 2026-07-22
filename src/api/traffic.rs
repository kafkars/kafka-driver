//! Semantic connection lanes that isolate Kafka head-of-line blocking.

/// The latency and ordering role of a request.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TrafficClass {
    /// Metadata, coordinator discovery, and future heartbeat work.
    Control,
    /// Admin and ordinary request/response work.
    Interactive,
    /// Produce and other throughput-oriented work.
    Bulk,
    /// Fetch and other deliberately long-held requests.
    LongPoll,
}
