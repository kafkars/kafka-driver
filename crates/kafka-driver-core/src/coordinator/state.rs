//! States containing only coordinator facts and discovery ownership valid together.

use crate::{CoordinatorEpoch, CoordinatorRoute, Moment, OperationId};

/// Why a discovery result must be followed by another external query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoordinatorFollowup {
    /// Explicit refresh demand arrived while discovery was already active.
    Refresh,
    /// A failed route was withdrawn after the active discovery had started.
    Revocation,
}

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
        /// Strongest reason another discovery must follow this result.
        followup: Option<CoordinatorFollowup>,
        /// Number of bounded transient retries already started in this pass.
        retries: u8,
    },
    /// One transient discovery rejection owns a positive bounded retry delay.
    Retrying {
        /// Previous route retained until a newer discovery succeeds.
        current: Option<CoordinatorRoute>,
        /// Failed discovery identity fencing the retry wake.
        operation_id: OperationId,
        /// Epoch retained for a successful retry result.
        target_epoch: CoordinatorEpoch,
        /// Strongest reason another discovery must follow this result.
        followup: Option<CoordinatorFollowup>,
        /// Number of bounded transient retries already authorized in this pass.
        retries: u8,
        /// Earliest driver-relative instant at which retry may start.
        at: Moment,
    },
    /// One discovered route is authoritative.
    Ready {
        /// Current coordinator route.
        route: CoordinatorRoute,
    },
}
