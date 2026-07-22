//! Valid lifecycle states and connection-local active-session ownership.

use crate::{AuthenticationState, ConnectionEpoch, EffectId, Moment, TimerId, TransportId};

use super::CloseReason;

/// Stable lifecycle name used by diagnostics and state inspection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionPhase {
    /// No external transport work has been requested.
    Dormant,
    /// An open-transport effect is outstanding.
    Opening,
    /// Initial API version negotiation is outstanding.
    Negotiating,
    /// SASL mechanism handshake or authentication exchange is outstanding.
    Authenticating,
    /// Calls may be admitted and responses are tracked in FIFO order.
    Ready,
    /// Existing calls may complete but new calls are rejected.
    Draining,
    /// A close-transport effect is outstanding.
    Closing,
    /// The epoch is terminal.
    Closed,
}

/// Immutable lifecycle snapshot containing only data valid in the current state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionState {
    /// No external transport work has been requested.
    Dormant {
        /// Epoch owned by the machine.
        epoch: ConnectionEpoch,
    },
    /// One open-transport effect is outstanding.
    Opening {
        /// Epoch being opened.
        epoch: ConnectionEpoch,
        /// Open effect identity.
        effect_id: EffectId,
        /// Reserved transport resource.
        transport_id: TransportId,
        /// Transport establishment deadline timer.
        deadline_timer: TimerId,
        /// Absolute driver-relative transport establishment deadline.
        deadline: Moment,
    },
    /// The transport is open and initial API negotiation is outstanding.
    Negotiating {
        /// Epoch being negotiated.
        epoch: ConnectionEpoch,
        /// Opened transport resource.
        transport_id: TransportId,
        /// Negotiation effect identity.
        effect_id: EffectId,
        /// Negotiation deadline timer.
        deadline_timer: TimerId,
        /// Absolute driver-relative negotiation deadline.
        deadline: Moment,
    },
    /// API negotiation completed and SASL authentication is outstanding.
    Authenticating {
        /// Epoch being authenticated.
        epoch: ConnectionEpoch,
        /// Opened transport resource.
        transport_id: TransportId,
        /// Secret-free child-machine state.
        authentication: AuthenticationState,
        /// Mutually supported APIs retained for this epoch.
        capabilities: usize,
    },
    /// The transport accepts new calls.
    Ready {
        /// Active connection epoch.
        epoch: ConnectionEpoch,
        /// Active transport resource.
        transport_id: TransportId,
        /// Calls currently awaiting write acceptance or a response.
        pending: usize,
        /// Mutually supported APIs retained for this epoch.
        capabilities: usize,
    },
    /// The transport is finishing already admitted calls.
    Draining {
        /// Active connection epoch.
        epoch: ConnectionEpoch,
        /// Active transport resource.
        transport_id: TransportId,
        /// Calls that must finish before transport closure.
        pending: usize,
        /// Mutually supported APIs retained for this epoch.
        capabilities: usize,
    },
    /// A close-transport effect is outstanding.
    Closing {
        /// Epoch being closed.
        epoch: ConnectionEpoch,
        /// Transport resource being closed.
        transport_id: TransportId,
        /// Policy reason for closure.
        reason: CloseReason,
    },
    /// The connection epoch is terminal.
    Closed {
        /// Terminal epoch.
        epoch: ConnectionEpoch,
        /// Reason the epoch ended.
        reason: CloseReason,
    },
}

impl ConnectionState {
    /// Returns the lifecycle name without state-specific data.
    pub const fn phase(self) -> ConnectionPhase {
        match self {
            Self::Dormant { .. } => ConnectionPhase::Dormant,
            Self::Opening { .. } => ConnectionPhase::Opening,
            Self::Negotiating { .. } => ConnectionPhase::Negotiating,
            Self::Authenticating { .. } => ConnectionPhase::Authenticating,
            Self::Ready { .. } => ConnectionPhase::Ready,
            Self::Draining { .. } => ConnectionPhase::Draining,
            Self::Closing { .. } => ConnectionPhase::Closing,
            Self::Closed { .. } => ConnectionPhase::Closed,
        }
    }
}
