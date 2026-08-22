//! External coordinator work emitted by deterministic discovery policy.

use crate::{CoordinatorEpoch, CoordinatorKey, Moment, OperationId};

/// External work requested by one coordinator transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoordinatorEffect {
    /// Fetches the broker endpoint for one exact key and target epoch.
    Find {
        /// Identity that returned work must echo.
        operation_id: OperationId,
        /// Exact coordinator key to request.
        key: CoordinatorKey,
        /// Epoch assigned only if this discovery succeeds.
        epoch: CoordinatorEpoch,
    },
    /// Retains discovery ownership until a positive retry delay elapses.
    WaitUntil {
        /// Failed discovery identity fencing the retry wake.
        operation_id: OperationId,
        /// Discovery epoch retained across retry.
        epoch: CoordinatorEpoch,
        /// Earliest driver-relative instant at which retry may start.
        at: Moment,
    },
    /// No further discovery epoch can be represented.
    EpochExhausted,
}
