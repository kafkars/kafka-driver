//! States containing only cluster facts and refresh ownership valid together.

use std::collections::VecDeque;

use crate::{MetadataGeneration, MetadataQuery, MetadataSnapshot, OperationId};

/// Current immutable metadata and at most one in-flight refresh.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataState {
    /// No cluster snapshot exists; the named generation remains unconsumed.
    Empty {
        /// Generation to assign to the first successful refresh.
        next_generation: MetadataGeneration,
    },
    /// One refresh is in flight while an optional previous snapshot remains usable.
    Refreshing {
        /// Previous immutable generation retained during refresh.
        current: Option<MetadataSnapshot>,
        /// Logical operation that owns the external fetch.
        operation_id: OperationId,
        /// Exact facts owned by the in-flight fetch.
        query: MetadataQuery,
        /// Generation reserved for a successful response.
        target_generation: MetadataGeneration,
        /// Distinct follow-up queries in first-demand order.
        queued: VecDeque<MetadataQuery>,
    },
    /// One coherent immutable generation is authoritative.
    Ready {
        /// Current cluster facts and generation-fenced routes.
        snapshot: MetadataSnapshot,
    },
}
