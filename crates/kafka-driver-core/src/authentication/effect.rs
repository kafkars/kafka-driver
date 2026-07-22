//! Secret-free external work and owner-visible authentication outcomes.

use kafka_wire_core::ApiVersion;

use crate::{ConnectionEpoch, EffectId, Moment, TimerId, TransportId};

use super::{AuthenticationFailure, AuthenticationRound, SaslMechanism};

/// One external action or terminal outcome requested by authentication policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationEffect {
    /// Registers the deadline for the whole authentication attempt.
    ScheduleDeadline {
        /// Connection epoch owning the deadline.
        epoch: ConnectionEpoch,
        /// Timer identity to echo when firing.
        timer_id: TimerId,
        /// Absolute driver-relative deadline.
        at: Moment,
    },
    /// Sends one generated `SaslHandshake` request.
    SendHandshake {
        /// Connection epoch owning the exchange.
        epoch: ConnectionEpoch,
        /// Open transport resource.
        transport_id: TransportId,
        /// High-level authentication effect identity.
        effect_id: EffectId,
        /// Configured mechanism without credentials.
        mechanism: SaslMechanism,
        /// Negotiated Kafka API version.
        version: ApiVersion,
    },
    /// Sends one generated `SaslAuthenticate` request.
    SendExchange {
        /// Connection epoch owning the exchange.
        epoch: ConnectionEpoch,
        /// Open transport resource.
        transport_id: TransportId,
        /// High-level authentication effect identity.
        effect_id: EffectId,
        /// One-based challenge-response round.
        round: AuthenticationRound,
        /// Negotiated Kafka API version.
        version: ApiVersion,
    },
    /// Removes a deadline that can no longer affect the attempt.
    CancelDeadline {
        /// Timer identity to remove.
        timer_id: TimerId,
    },
    /// Reports successful completion to the connection owner.
    Succeeded,
    /// Reports a sanitized permanent failure to the connection owner.
    Failed {
        /// Secret-free terminal failure category.
        failure: AuthenticationFailure,
    },
}
