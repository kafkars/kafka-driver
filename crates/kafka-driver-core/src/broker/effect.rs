//! External work requested by the long-lived broker machine.

use crate::{ConnectionEpoch, Moment, TimerId};

/// Ordered external work emitted by one broker transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerEffect {
    /// Creates and starts a new connection child for this generation.
    OpenConnection {
        /// Fresh connection generation to create.
        epoch: ConnectionEpoch,
    },
    /// Schedules the bounded delay before the next connection generation.
    ScheduleReconnect {
        /// Terminal generation whose failure authorized this retry.
        failed_epoch: ConnectionEpoch,
        /// Timer identity to echo when the delay expires.
        timer_id: TimerId,
        /// Absolute driver-relative reconnect deadline.
        at: Moment,
    },
    /// Schedules a bounded delay before retrying endpoint resolution.
    ScheduleEndpointRefreshRetry {
        /// Connection generation whose address pass was exhausted.
        failed_epoch: ConnectionEpoch,
        /// Timer identity reserved for this refresh retry.
        timer_id: TimerId,
        /// Absolute driver-relative retry deadline.
        at: Moment,
    },
    /// Cancels a reconnect timer because shutdown won ownership.
    CancelReconnect {
        /// Timer identity whose ownership is ending.
        timer_id: TimerId,
    },
    /// Cancels an endpoint-refresh timer because shutdown won ownership.
    CancelEndpointRefreshRetry {
        /// Timer identity whose refresh ownership is ending.
        timer_id: TimerId,
    },
    /// Asks the current connection child to drain and close.
    DrainConnection {
        /// Current generation that must drain.
        epoch: ConnectionEpoch,
    },
}
