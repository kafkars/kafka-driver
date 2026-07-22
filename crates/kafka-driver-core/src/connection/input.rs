//! Data-only inputs accepted by one deterministic connection machine.

use crate::{
    AuthenticationAttempt, AuthenticationInput, CallId, ConnectionEpoch, EffectId, Moment, TimerId,
    TransportId,
};

use super::{
    CorrelationId, NegotiationAttempt, NegotiationFailure, ResponseFault, TransportFailure,
};
use crate::NegotiatedCapabilities;

/// One internal command or external outcome applied to the machine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectionInput {
    /// Starts opening the epoch's transport resource.
    Start {
        /// Identity of the requested open effect.
        effect_id: EffectId,
        /// Identity reserved for the transport resource.
        transport_id: TransportId,
        /// Timer identity reserved for transport establishment.
        deadline_timer: TimerId,
        /// Absolute driver-relative transport establishment deadline.
        deadline: Moment,
    },
    /// Reports that the requested transport opened successfully.
    TransportOpened {
        /// Epoch echoed from the open effect.
        epoch: ConnectionEpoch,
        /// Open effect identity being completed.
        effect_id: EffectId,
        /// Opened transport resource.
        transport_id: TransportId,
        /// Reserved identities and bounded timing for the `ApiVersions` exchange.
        negotiation: NegotiationAttempt,
    },
    /// Reports that the requested transport could not open.
    TransportOpenFailed {
        /// Epoch echoed from the open effect.
        epoch: ConnectionEpoch,
        /// Open effect identity being completed.
        effect_id: EffectId,
        /// Failed transport resource.
        transport_id: TransportId,
        /// Sanitized external failure category.
        failure: TransportFailure,
    },
    /// Reports the mutually supported APIs selected from `ApiVersions`.
    ApiVersionsNegotiated {
        /// Epoch echoed from the negotiation effect.
        epoch: ConnectionEpoch,
        /// Transport echoed from the negotiation effect.
        transport_id: TransportId,
        /// Negotiation effect being completed.
        effect_id: EffectId,
        /// Immutable capability set for this connection epoch.
        capabilities: NegotiatedCapabilities,
    },
    /// Reports capabilities and starts required connection authentication.
    ApiVersionsNegotiatedWithAuthentication {
        /// Epoch echoed from the negotiation effect.
        epoch: ConnectionEpoch,
        /// Transport echoed from the negotiation effect.
        transport_id: TransportId,
        /// Negotiation effect being completed.
        effect_id: EffectId,
        /// Immutable capability set for this connection epoch.
        capabilities: NegotiatedCapabilities,
        /// Reserved identities and timing for the authentication phase.
        authentication: AuthenticationAttempt,
    },
    /// Reports terminal failure of the initial `ApiVersions` exchange.
    ApiVersionsFailed {
        /// Epoch echoed from the negotiation effect.
        epoch: ConnectionEpoch,
        /// Transport echoed from the negotiation effect.
        transport_id: TransportId,
        /// Negotiation effect being failed.
        effect_id: EffectId,
        /// Sanitized negotiation failure category.
        failure: NegotiationFailure,
    },
    /// Applies one secret-free outcome to the connection-owned authentication child.
    Authentication {
        /// Child-machine input carrying epoch, effect, round, or timer identity.
        input: AuthenticationInput,
    },
    /// Admits one call for ordered write and response tracking.
    Submit {
        /// Public logical call identity.
        call_id: CallId,
        /// Effect identity reserved for writing this call.
        write_effect: EffectId,
        /// Timer identity reserved for this call's deadline.
        deadline_timer: TimerId,
        /// Current driver-relative time.
        now: Moment,
        /// Absolute driver-relative call deadline.
        deadline: Moment,
    },
    /// Reports that the writer accepted the complete encoded frame.
    WriteSubmitted {
        /// Connection epoch echoed from the write effect.
        epoch: ConnectionEpoch,
        /// Transport resource echoed from the write effect.
        transport_id: TransportId,
        /// Write effect being completed.
        effect_id: EffectId,
    },
    /// Aborts one call that local work rejected before writer acceptance.
    AbortUnsentCall {
        /// Connection epoch echoed from the write effect.
        epoch: ConnectionEpoch,
        /// Transport resource echoed from the write effect.
        transport_id: TransportId,
        /// Public call identity whose unsent work is being aborted.
        call_id: CallId,
        /// Write effect that was never accepted by the writer.
        effect_id: EffectId,
    },
    /// Reports that a requested write failed before response completion.
    WriteFailed {
        /// Connection epoch echoed from the write effect.
        epoch: ConnectionEpoch,
        /// Transport resource echoed from the write effect.
        transport_id: TransportId,
        /// Write effect being completed.
        effect_id: EffectId,
        /// Sanitized transport failure category.
        failure: TransportFailure,
    },
    /// Reports the correlation header of the next complete response frame.
    ResponseReceived {
        /// Connection epoch that produced the frame.
        epoch: ConnectionEpoch,
        /// Transport resource that produced the frame.
        transport_id: TransportId,
        /// Correlation decoded from the response header.
        correlation_id: CorrelationId,
    },
    /// Reports a complete response that could not yield a policy-safe correlation.
    ResponseRejected {
        /// Connection epoch that produced the frame.
        epoch: ConnectionEpoch,
        /// Transport resource that produced the frame.
        transport_id: TransportId,
        /// Sanitized reason header inspection could not continue.
        fault: ResponseFault,
    },
    /// Reports a call deadline timer firing.
    DeadlineElapsed {
        /// Connection epoch echoed from the timer effect.
        epoch: ConnectionEpoch,
        /// Timer identity that fired.
        timer_id: TimerId,
        /// Current driver-relative time.
        now: Moment,
    },
    /// Stops new admission and drains existing pending calls.
    BeginDrain,
    /// Reports that an external transport resource has closed.
    TransportClosed {
        /// Epoch that owned the transport.
        epoch: ConnectionEpoch,
        /// Closed transport identity.
        transport_id: TransportId,
        /// Sanitized close category.
        failure: TransportFailure,
    },
}

impl ConnectionInput {
    pub(super) const fn kind(&self) -> ConnectionInputKind {
        match self {
            Self::Start { .. } => ConnectionInputKind::Start,
            Self::TransportOpened { .. } => ConnectionInputKind::TransportOpened,
            Self::TransportOpenFailed { .. } => ConnectionInputKind::TransportOpenFailed,
            Self::ApiVersionsNegotiated { .. } => ConnectionInputKind::ApiVersionsNegotiated,
            Self::ApiVersionsNegotiatedWithAuthentication { .. } => {
                ConnectionInputKind::ApiVersionsNegotiatedWithAuthentication
            }
            Self::ApiVersionsFailed { .. } => ConnectionInputKind::ApiVersionsFailed,
            Self::Authentication { .. } => ConnectionInputKind::Authentication,
            Self::Submit { .. } => ConnectionInputKind::Submit,
            Self::WriteSubmitted { .. } => ConnectionInputKind::WriteSubmitted,
            Self::AbortUnsentCall { .. } => ConnectionInputKind::AbortUnsentCall,
            Self::WriteFailed { .. } => ConnectionInputKind::WriteFailed,
            Self::ResponseReceived { .. } => ConnectionInputKind::ResponseReceived,
            Self::ResponseRejected { .. } => ConnectionInputKind::ResponseRejected,
            Self::DeadlineElapsed { .. } => ConnectionInputKind::DeadlineElapsed,
            Self::BeginDrain => ConnectionInputKind::BeginDrain,
            Self::TransportClosed { .. } => ConnectionInputKind::TransportClosed,
        }
    }
}

/// Sanitized input name retained by transition diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionInputKind {
    /// `ConnectionInput::Start`.
    Start,
    /// `ConnectionInput::TransportOpened`.
    TransportOpened,
    /// `ConnectionInput::TransportOpenFailed`.
    TransportOpenFailed,
    /// `ConnectionInput::ApiVersionsNegotiated`.
    ApiVersionsNegotiated,
    /// `ConnectionInput::ApiVersionsNegotiatedWithAuthentication`.
    ApiVersionsNegotiatedWithAuthentication,
    /// `ConnectionInput::ApiVersionsFailed`.
    ApiVersionsFailed,
    /// `ConnectionInput::Authentication`.
    Authentication,
    /// `ConnectionInput::Submit`.
    Submit,
    /// `ConnectionInput::WriteSubmitted`.
    WriteSubmitted,
    /// `ConnectionInput::AbortUnsentCall`.
    AbortUnsentCall,
    /// `ConnectionInput::WriteFailed`.
    WriteFailed,
    /// `ConnectionInput::ResponseReceived`.
    ResponseReceived,
    /// `ConnectionInput::ResponseRejected`.
    ResponseRejected,
    /// `ConnectionInput::DeadlineElapsed`.
    DeadlineElapsed,
    /// `ConnectionInput::BeginDrain`.
    BeginDrain,
    /// `ConnectionInput::TransportClosed`.
    TransportClosed,
}
