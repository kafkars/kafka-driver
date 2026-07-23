//! Data-only commands and connection outcomes accepted by broker policy.

use crate::{AuthenticationFailure, ConnectionEpoch, DnsFailure, Moment, TimerId};

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

/// External identities, time, and entropy for one endpoint-refresh retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EndpointRefreshSchedule {
    pub(super) timer_id: TimerId,
    pub(super) now: Moment,
    pub(super) jitter: JitterSample,
}

impl EndpointRefreshSchedule {
    /// Creates a retry schedule from reactor-owned observations.
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
    /// Reports that a terminal connection exhausted every resolved address.
    EndpointExhausted {
        /// Failed connection generation awaiting newer endpoint evidence.
        epoch: ConnectionEpoch,

        /// Reserved retry identity, time, and entropy held until refresh.
        reconnect: ReconnectSchedule,
    },
    /// Reports that external DNS ownership began for a suspended reconnect.
    EndpointRefreshStarted {
        /// Failed generation whose endpoint is being refreshed.
        failed_epoch: ConnectionEpoch,
    },
    /// Returns DNS ownership when bounded worker admission could not proceed.
    EndpointRefreshDeferred {
        /// Failed generation whose endpoint refresh remains pending.
        failed_epoch: ConnectionEpoch,
    },
    /// Reports one sanitized endpoint-refresh failure.
    EndpointRefreshFailed {
        /// Failed connection generation awaiting endpoint evidence.
        failed_epoch: ConnectionEpoch,

        /// Sanitized resolver failure classification.
        failure: DnsFailure,

        /// External identities, time, and entropy available for retry.
        retry: EndpointRefreshSchedule,
    },
    /// Reports one endpoint-refresh retry timer firing.
    EndpointRefreshRetryElapsed {
        /// Failed connection generation echoed from the timer.
        failed_epoch: ConnectionEpoch,

        /// Endpoint-refresh timer identity that fired.
        timer_id: TimerId,

        /// Current driver-relative time.
        now: Moment,
    },
    /// Reports that newer endpoint evidence is ready for a suspended reconnect.
    EndpointRefreshed {
        /// Failed generation whose endpoint refresh produced the evidence.
        failed_epoch: ConnectionEpoch,

        /// Current driver-relative time used to preserve the original backoff.
        now: Moment,
    },
    /// Reports a permanent authentication rejection for the named generation.
    ConnectionRejected {
        /// Terminal connection generation.
        epoch: ConnectionEpoch,
        /// Sanitized authentication failure retained as terminal policy.
        failure: AuthenticationFailure,
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
