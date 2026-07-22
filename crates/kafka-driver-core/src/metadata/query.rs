//! Explicit cluster or single-topic Metadata RPC scope and queue bounds.

use std::num::NonZeroUsize;

use crate::TopicName;

const DEFAULT_PENDING_QUERIES: NonZeroUsize = nonzero(256);

/// Facts requested by one generated Metadata RPC.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataQuery {
    /// Refreshes broker membership and controller ownership without requesting topic data.
    Cluster,
    /// Refreshes leader facts for exactly one topic.
    Topic(TopicName),
}

/// Maximum distinct Metadata queries waiting behind one in-flight RPC.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataQueryLimits {
    pending_queries: NonZeroUsize,
}

impl MetadataQueryLimits {
    /// Creates an explicit pending-query bound.
    pub const fn new(pending_queries: NonZeroUsize) -> Self {
        Self { pending_queries }
    }

    /// Returns the maximum distinct queued queries.
    pub const fn pending_queries(self) -> NonZeroUsize {
        self.pending_queries
    }

    /// Returns the reference default without trait dispatch.
    pub const fn defaults() -> Self {
        Self::new(DEFAULT_PENDING_QUERIES)
    }
}

impl Default for MetadataQueryLimits {
    fn default() -> Self {
        Self::defaults()
    }
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("metadata query default must be nonzero");
    };
    value
}
