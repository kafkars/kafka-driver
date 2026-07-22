//! Data-only commands and connection outcomes accepted by broker policy.

use crate::{ConnectionEpoch, Moment, TimerId};

use super::JitterSample;

/// External identities and time used to schedule one reconnect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconnectSchedule {
    pub(super) timer_id: TimerId,
    pub(super) now: Moment,
    pub(super) jitter: JitterSample,
}

impl ReconnectSchedule {
    /// Creates a reconnect schedule from reactor-owned observations.
    pub const fn new(timer_id: TimerId, now: Moment, jitter: JitterSample) -> Self {
        Self {
            timer_id,
            now,
            jitter,
        }
    }
}

/// One command or connection-child outcome applied to broker policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerInput {
    /// Starts the initial configured connection generation.
    Start,
    /// Reports that negotiation made the named connection ready for calls.
    ConnectionReady {
        /// Connection generation that completed setup.
        epoch: ConnectionEpoch,
    },
    /// Reports an unexpected terminal connection and supplies retry resources.
    ConnectionFailed {
        /// Unexpectedly terminal connection generation.
        epoch: ConnectionEpoch,
        /// External identities, time, and entropy for retry.
        reconnect: ReconnectSchedule,
    },
    /// Reports that the connection closed under requested drain.
    ConnectionDrained {
        /// Connection generation that finished draining.
        epoch: ConnectionEpoch,
    },
    /// Reports one reconnect timer firing at driver-relative time.
    ReconnectElapsed {
        /// Failed generation echoed from the timer effect.
        failed_epoch: ConnectionEpoch,
        /// Timer identity that fired.
        timer_id: TimerId,
        /// Current driver-relative time.
        now: Moment,
    },
    /// Stops retry and asks any current connection to drain.
    BeginDrain,
}
