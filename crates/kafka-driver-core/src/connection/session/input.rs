//! Transport-independent observations accepted by one Kafka session.

use crate::{
    AuthenticationFailure, AuthenticationRound, ExchangeOutcome, Moment, NegotiatedCapabilities,
    NegotiationFailure,
};

use super::KafkaSessionProtocolFailure;

/// Current time and absolute deadline for one session-establishment stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaSessionDeadline {
    pub(super) now: Moment,
    pub(super) deadline: Moment,
}

impl KafkaSessionDeadline {
    /// Creates a bounded session-stage deadline.
    pub const fn new(now: Moment, deadline: Moment) -> Self {
        Self { now, deadline }
    }

    /// Returns the absolute driver-relative deadline.
    pub const fn deadline(self) -> Moment {
        self.deadline
    }
}

/// One protocol or lifecycle observation applied to a Kafka session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KafkaSessionInput {
    /// Reports an application-ready transport and starts `ApiVersions`.
    TransportOpened {
        /// Time bound for the complete `ApiVersions` stage.
        deadline: KafkaSessionDeadline,
    },
    /// Reports negotiated capabilities for an unauthenticated session.
    ApiVersionsSucceeded {
        /// Immutable capability set selected for this session.
        capabilities: NegotiatedCapabilities,
    },
    /// Reports capabilities and starts configured SASL authentication.
    ApiVersionsSucceededWithAuthentication {
        /// Immutable capability set selected for this session.
        capabilities: NegotiatedCapabilities,
        /// Time bound for the complete SASL stage.
        deadline: KafkaSessionDeadline,
    },
    /// Reports terminal failure of the `ApiVersions` stage.
    ApiVersionsFailed {
        /// Sanitized negotiation failure.
        failure: NegotiationFailure,
    },
    /// Reports acceptance of the configured SASL mechanism.
    AuthenticationHandshakeSucceeded,
    /// Reports terminal rejection of the configured SASL mechanism.
    AuthenticationHandshakeFailed {
        /// Sanitized authentication failure.
        failure: AuthenticationFailure,
    },
    /// Reports one bounded SASL challenge-response outcome.
    AuthenticationExchangeCompleted {
        /// One-based exchange round being completed.
        round: AuthenticationRound,
        /// Sanitized mechanism outcome.
        outcome: ExchangeOutcome,
    },
    /// Reports the session-stage deadline at current driver-relative time.
    DeadlineElapsed {
        /// Current driver-relative time.
        now: Moment,
    },
    /// Stops ordinary admission and begins bounded session drain.
    BeginDrain,
    /// Reports that every already admitted operation has drained.
    Drained,
    /// Reports a session-establishment protocol fault.
    ProtocolFailed {
        /// Sanitized protocol fault.
        failure: KafkaSessionProtocolFailure,
    },
    /// Confirms that the session owner released its transport.
    Closed,
}
