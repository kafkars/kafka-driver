//! Data-only authentication commands and sanitized external outcomes.

use crate::{ConnectionEpoch, EffectId, Moment, TimerId, TransportId};

use super::{AuthenticationFailure, AuthenticationRound};

/// Reserved identity and time bounds for one complete authentication attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthenticationAttempt {
    pub(super) effect_id: EffectId,
    pub(super) deadline_timer: TimerId,
    pub(super) now: Moment,
    pub(super) deadline: Moment,
}

impl AuthenticationAttempt {
    /// Creates one bounded authentication attempt.
    pub const fn new(
        effect_id: EffectId,
        deadline_timer: TimerId,
        now: Moment,
        deadline: Moment,
    ) -> Self {
        Self {
            effect_id,
            deadline_timer,
            now,
            deadline,
        }
    }
}

/// Sanitized result of one mechanism exchange response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeOutcome {
    /// The mechanism requires another client message.
    Continue,
    /// The broker and client both accepted the authenticated transcript.
    Succeeded,
    /// The exchange ended terminally without retaining broker text or bytes.
    Failed(AuthenticationFailure),
}

/// One internal command or external SASL outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationInput {
    /// Starts the deadline and mechanism handshake.
    Start {
        /// Reserved identities and driver-relative timing.
        attempt: AuthenticationAttempt,
    },
    /// Reports acceptance of the configured mechanism.
    HandshakeAccepted {
        /// Epoch echoed from the handshake effect.
        epoch: ConnectionEpoch,
        /// Transport echoed from the handshake effect.
        transport_id: TransportId,
        /// Authentication effect being completed.
        effect_id: EffectId,
    },
    /// Reports terminal mechanism-handshake failure.
    HandshakeFailed {
        /// Epoch echoed from the handshake effect.
        epoch: ConnectionEpoch,
        /// Transport echoed from the handshake effect.
        transport_id: TransportId,
        /// Authentication effect being failed.
        effect_id: EffectId,
        /// Secret-free terminal category.
        failure: AuthenticationFailure,
    },
    /// Reports the sanitized outcome of one authentication response.
    ExchangeCompleted {
        /// Epoch echoed from the exchange effect.
        epoch: ConnectionEpoch,
        /// Transport echoed from the exchange effect.
        transport_id: TransportId,
        /// Authentication effect being completed.
        effect_id: EffectId,
        /// Exchange round echoed from the effect.
        round: AuthenticationRound,
        /// Sanitized mechanism outcome.
        outcome: ExchangeOutcome,
    },
    /// Reports the authentication deadline timer firing.
    DeadlineElapsed {
        /// Epoch echoed from the deadline effect.
        epoch: ConnectionEpoch,
        /// Timer identity that fired.
        timer_id: TimerId,
        /// Current driver-relative time.
        now: Moment,
    },
}
