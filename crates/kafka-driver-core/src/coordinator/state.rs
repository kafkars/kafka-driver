//! States containing only coordinator facts and discovery ownership valid together.

use crate::{CoordinatorEpoch, CoordinatorRoute, OperationId};

/// Current route and at most one in-flight discovery for one coordinator key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoordinatorState {
    /// No successful discovery exists; the named epoch remains unconsumed.
    Unknown {
        /// Epoch reserved for the next successful discovery.
        next_epoch: CoordinatorEpoch,
    },
    /// One discovery is in flight while an older route may remain usable.
    Discovering {
        /// Previous route retained until a newer discovery succeeds.
        current: Option<CoordinatorRoute>,
        /// Logical operation that owns the external request.
        operation_id: OperationId,
        /// Epoch reserved for a successful result.
        target_epoch: CoordinatorEpoch,
        /// Whether one explicitly newer discovery must follow this result.
        refresh_pending: bool,
    },
    /// One discovered route is authoritative.
    Ready {
        /// Current coordinator route.
        route: CoordinatorRoute,
    },
}
