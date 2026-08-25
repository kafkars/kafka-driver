//! Secret-free, transport-independent Kafka session states.

use crate::{
    AuthenticationFailure, AuthenticationRound, Moment, NegotiatedCapabilities, NegotiationFailure,
};

/// Stable lifecycle name for one Kafka protocol session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaSessionPhase {
    /// The session has not observed an application-ready transport.
    AwaitingTransport,
    /// `ApiVersions` is outstanding.
    Negotiating,
    /// SASL handshake or exchange work is outstanding.
    Authenticating,
    /// Ordinary Kafka operations may be admitted.
    Ready,
    /// Existing operations may finish but new work is rejected.
    Draining,
    /// Semantic closure has been requested from the owner.
    Closing,
    /// The owner confirmed terminal release.
    Closed,
}

/// Sanitized protocol fault observed while establishing a Kafka session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaSessionProtocolFailure {
    /// A complete session response could not be decoded safely.
    Malformed,
    /// A response or stage outcome was unexpected for current policy.
    Unexpected,
}

/// Why a semantic Kafka session is closing or closed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaSessionCloseReason {
    /// Every admitted operation drained normally.
    Drained,
    /// The owner requested shutdown.
    Requested,
    /// Initial API negotiation failed.
    NegotiationFailed(NegotiationFailure),
    /// Configured SASL authentication failed.
    AuthenticationFailed(AuthenticationFailure),
    /// A session-establishment response violated protocol policy.
    ProtocolFailed(KafkaSessionProtocolFailure),
    /// The owner released the underlying transport unexpectedly.
    TransportClosed,
}

/// Secret-free progress within the configured SASL stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaSessionAuthenticationState {
    /// `SaslHandshake` is outstanding.
    Handshaking {
        /// Absolute deadline for the complete SASL stage.
        deadline: Moment,
    },
    /// One `SaslAuthenticate` exchange is outstanding.
    Exchanging {
        /// One-based exchange round.
        round: AuthenticationRound,
        /// Absolute deadline for the complete SASL stage.
        deadline: Moment,
    },
}

/// Immutable snapshot containing only data valid in the current session state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaSessionState {
    /// No application-ready transport has been observed.
    AwaitingTransport,
    /// Initial API negotiation is outstanding.
    Negotiating {
        /// Absolute negotiation deadline.
        deadline: Moment,
    },
    /// Configured SASL authentication is outstanding.
    Authenticating {
        /// Current SASL stage.
        authentication: KafkaSessionAuthenticationState,
        /// Mutually supported APIs retained for this session.
        capabilities: usize,
    },
    /// Ordinary operations may be admitted.
    Ready {
        /// Mutually supported APIs retained for this session.
        capabilities: usize,
    },
    /// Existing operations may finish but new work is rejected.
    Draining {
        /// Mutually supported APIs retained for this session.
        capabilities: usize,
    },
    /// Semantic closure has been requested.
    Closing {
        /// Policy reason for closure.
        reason: KafkaSessionCloseReason,
    },
    /// The owner confirmed terminal release.
    Closed {
        /// Policy reason retained after release.
        reason: KafkaSessionCloseReason,
    },
}

impl KafkaSessionState {
    /// Returns the stable lifecycle name without state-specific data.
    pub const fn phase(self) -> KafkaSessionPhase {
        match self {
            Self::AwaitingTransport => KafkaSessionPhase::AwaitingTransport,
            Self::Negotiating { .. } => KafkaSessionPhase::Negotiating,
            Self::Authenticating { .. } => KafkaSessionPhase::Authenticating,
            Self::Ready { .. } => KafkaSessionPhase::Ready,
            Self::Draining { .. } => KafkaSessionPhase::Draining,
            Self::Closing { .. } => KafkaSessionPhase::Closing,
            Self::Closed { .. } => KafkaSessionPhase::Closed,
        }
    }
}

#[derive(Debug)]
pub(super) enum AuthenticationStage {
    Handshaking {
        deadline: Moment,
    },
    Exchanging {
        round: AuthenticationRound,
        deadline: Moment,
    },
}

#[derive(Debug)]
pub(super) enum StateData {
    AwaitingTransport,
    Negotiating {
        deadline: Moment,
    },
    Authenticating {
        capabilities: NegotiatedCapabilities,
        protocol: crate::SaslProtocol,
        stage: AuthenticationStage,
    },
    Ready {
        capabilities: NegotiatedCapabilities,
    },
    Draining {
        capabilities: NegotiatedCapabilities,
    },
    Closing {
        reason: KafkaSessionCloseReason,
    },
    Closed {
        reason: KafkaSessionCloseReason,
    },
}

impl StateData {
    pub(super) fn snapshot(&self) -> KafkaSessionState {
        match self {
            Self::AwaitingTransport => KafkaSessionState::AwaitingTransport,
            Self::Negotiating { deadline } => KafkaSessionState::Negotiating {
                deadline: *deadline,
            },
            Self::Authenticating {
                capabilities,
                stage,
                ..
            } => KafkaSessionState::Authenticating {
                authentication: stage.snapshot(),
                capabilities: capabilities.len(),
            },
            Self::Ready { capabilities } => KafkaSessionState::Ready {
                capabilities: capabilities.len(),
            },
            Self::Draining { capabilities } => KafkaSessionState::Draining {
                capabilities: capabilities.len(),
            },
            Self::Closing { reason } => KafkaSessionState::Closing { reason: *reason },
            Self::Closed { reason } => KafkaSessionState::Closed { reason: *reason },
        }
    }
}

impl AuthenticationStage {
    fn snapshot(&self) -> KafkaSessionAuthenticationState {
        match self {
            Self::Handshaking { deadline } => KafkaSessionAuthenticationState::Handshaking {
                deadline: *deadline,
            },
            Self::Exchanging { round, deadline } => KafkaSessionAuthenticationState::Exchanging {
                round: *round,
                deadline: *deadline,
            },
        }
    }
}
