//! Semantic work emitted by transport-independent Kafka session policy.

use kafka_wire_core::ApiVersion;

use crate::{AuthenticationRound, Moment, SaslMechanism};

use super::KafkaSessionCloseReason;

/// One semantic action requested by a Kafka session transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaSessionEffect {
    /// Starts the initial `ApiVersions` request as session work.
    StartApiVersions {
        /// Absolute deadline for negotiation completion.
        deadline: Moment,
    },
    /// Starts the configured SASL mechanism handshake.
    StartAuthenticationHandshake {
        /// Configured mechanism without credentials.
        mechanism: SaslMechanism,
        /// Negotiated `SaslHandshake` version.
        version: ApiVersion,
        /// Absolute deadline for the complete SASL stage.
        deadline: Moment,
    },
    /// Starts one SASL challenge-response exchange.
    StartAuthenticationExchange {
        /// One-based exchange round.
        round: AuthenticationRound,
        /// Negotiated `SaslAuthenticate` version.
        version: ApiVersion,
        /// Absolute deadline shared by the complete SASL stage.
        deadline: Moment,
    },
    /// Reschedules an early session-deadline observation.
    RescheduleDeadline {
        /// Absolute session-stage deadline.
        at: Moment,
    },
    /// Cancels a session-stage deadline that can no longer affect state.
    CancelDeadline,
    /// Announces that ordinary operations may be admitted.
    SessionReady,
    /// Asks the owner to drain already admitted operations.
    BeginDrain,
    /// Asks the owner to close the semantic session.
    CloseSession {
        /// Policy reason for closure.
        reason: KafkaSessionCloseReason,
    },
}
