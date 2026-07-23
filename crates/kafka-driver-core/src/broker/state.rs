//! Valid long-lived broker states across replaceable connection generations.

use crate::{AuthenticationFailure, ConnectionEpoch, Moment, TimerId};

use super::RetryOrdinal;

/// Stable broker lifecycle name for observation and tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerPhase {
    /// No connection generation has been requested.
    Dormant,
    /// One connection child is opening, negotiating, or authenticating.
    Connecting,
    /// The current connection child accepts calls.
    Available,
    /// A bounded reconnect delay is pending.
    Backoff,
    /// Reconnect is suspended until newer endpoint evidence arrives.
    Refreshing,
    /// Shutdown is waiting for the current connection child.
    Draining,
    /// The broker owner is terminal and will not reconnect.
    Closed,
}

/// Why this broker owner will create no further connection generations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerCloseReason {
    /// Host shutdown permanently ended retry.
    Requested,
    /// No fresh connection generation could be represented.
    EpochExhausted,
    /// No further retry ordinal could be represented.
    RetryExhausted,
    /// The reconnect deadline exceeded driver-relative time.
    ClockOverflow,
    /// Broker authentication failed permanently for this configuration.
    AuthenticationFailed(AuthenticationFailure),
}

/// Immutable broker lifecycle snapshot containing only state-valid data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerState {
    /// The initial generation is reserved but has not started.
    Dormant {
        /// First connection generation to create.
        initial_epoch: ConnectionEpoch,
    },
    /// One connection generation is being established.
    Connecting {
        /// Current connection generation.
        epoch: ConnectionEpoch,
        /// Retry that created it, or `None` for the initial connection.
        retry: Option<RetryOrdinal>,
    },
    /// One negotiated connection generation accepts calls.
    Available {
        /// Current ready connection generation.
        epoch: ConnectionEpoch,
    },
    /// The next fresh connection generation is delayed.
    Backoff {
        /// Generation whose failure authorized retry.
        failed_epoch: ConnectionEpoch,
        /// Fresh generation selected before external timer work.
        next_epoch: ConnectionEpoch,
        /// One-based retry ordinal controlling the delay cap.
        retry: RetryOrdinal,
        /// Owned reconnect timer identity.
        timer_id: TimerId,
        /// Absolute driver-relative reconnect deadline.
        deadline: Moment,
    },
    /// Every known address failed and reconnect awaits a fresh resolver result.
    Refreshing {
        /// Generation whose address pass was exhausted.
        failed_epoch: ConnectionEpoch,
        /// Fresh generation reserved before external DNS work.
        next_epoch: ConnectionEpoch,
        /// Retry ordinal preserved across endpoint refresh.
        retry: RetryOrdinal,
        /// Reserved reconnect identity not scheduled while DNS is outstanding.
        timer_id: TimerId,
        /// Original failure-relative reconnect deadline.
        deadline: Moment,
    },
    /// Host shutdown is waiting for one current child to close.
    Draining {
        /// Connection generation asked to drain.
        epoch: ConnectionEpoch,
    },
    /// Retry and connection creation have ended permanently.
    Closed {
        /// Policy reason no future generation may open.
        reason: BrokerCloseReason,
    },
}

impl BrokerState {
    /// Returns the lifecycle name without state-specific data.
    pub const fn phase(self) -> BrokerPhase {
        match self {
            Self::Dormant { .. } => BrokerPhase::Dormant,
            Self::Connecting { .. } => BrokerPhase::Connecting,
            Self::Available { .. } => BrokerPhase::Available,
            Self::Backoff { .. } => BrokerPhase::Backoff,
            Self::Refreshing { .. } => BrokerPhase::Refreshing,
            Self::Draining { .. } => BrokerPhase::Draining,
            Self::Closed { .. } => BrokerPhase::Closed,
        }
    }
}
