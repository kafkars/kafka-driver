//! External coordinator work emitted by deterministic discovery policy.

use crate::{CoordinatorEpoch, CoordinatorKey, OperationId};

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
    /// No further discovery epoch can be represented.
    EpochExhausted,
}
