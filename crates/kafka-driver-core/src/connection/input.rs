//! Data-only inputs accepted by one deterministic connection machine.

use crate::{CallId, ConnectionEpoch, EffectId, Moment, TimerId, TransportId};

use super::{CorrelationId, ResponseFault, TransportFailure};

/// One internal command or external outcome applied to the machine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionInput {
    /// Starts opening the epoch's transport resource.
    Start {
        /// Identity of the requested open effect.
        effect_id: EffectId,
        /// Identity reserved for the transport resource.
        transport_id: TransportId,
    },
    /// Reports that the requested transport opened successfully.
    TransportOpened {
        /// Epoch echoed from the open effect.
        epoch: ConnectionEpoch,
        /// Open effect identity being completed.
        effect_id: EffectId,
        /// Opened transport resource.
        transport_id: TransportId,
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
    pub(super) const fn kind(self) -> ConnectionInputKind {
        match self {
            Self::Start { .. } => ConnectionInputKind::Start,
            Self::TransportOpened { .. } => ConnectionInputKind::TransportOpened,
            Self::TransportOpenFailed { .. } => ConnectionInputKind::TransportOpenFailed,
            Self::Submit { .. } => ConnectionInputKind::Submit,
            Self::WriteSubmitted { .. } => ConnectionInputKind::WriteSubmitted,
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
    /// `ConnectionInput::Submit`.
    Submit,
    /// `ConnectionInput::WriteSubmitted`.
    WriteSubmitted,
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
