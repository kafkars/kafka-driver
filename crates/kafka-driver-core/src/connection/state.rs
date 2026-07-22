//! Valid lifecycle states and connection-local active-session ownership.

use crate::{ConnectionEpoch, EffectId, Moment, TimerId, TransportId};

use super::{ActiveConnection, ActiveMode, CloseReason};

/// Stable lifecycle name used by diagnostics and state inspection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionPhase {
    /// No external transport work has been requested.
    Dormant,
    /// An open-transport effect is outstanding.
    Opening,
    /// Initial API version negotiation is outstanding.
    Negotiating,
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
            Self::Ready { .. } => ConnectionPhase::Ready,
            Self::Draining { .. } => ConnectionPhase::Draining,
            Self::Closing { .. } => ConnectionPhase::Closing,
            Self::Closed { .. } => ConnectionPhase::Closed,
        }
    }
}

pub(super) enum StateData {
    Dormant {
        epoch: ConnectionEpoch,
    },
    Opening {
        epoch: ConnectionEpoch,
        effect_id: EffectId,
        transport_id: TransportId,
    },
    Negotiating {
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        effect_id: EffectId,
        deadline_timer: TimerId,
        deadline: Moment,
    },
    Active {
        mode: ActiveMode,
        connection: ActiveConnection,
    },
    Closing {
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        reason: CloseReason,
    },
    Closed {
        epoch: ConnectionEpoch,
        reason: CloseReason,
    },
}

impl StateData {
    pub(super) const fn epoch(&self) -> ConnectionEpoch {
        match self {
            Self::Dormant { epoch }
            | Self::Opening { epoch, .. }
            | Self::Negotiating { epoch, .. }
            | Self::Closing { epoch, .. }
            | Self::Closed { epoch, .. } => *epoch,
            Self::Active { connection, .. } => connection.epoch,
        }
    }

    pub(super) const fn phase(&self) -> ConnectionPhase {
        match self {
            Self::Dormant { .. } => ConnectionPhase::Dormant,
            Self::Opening { .. } => ConnectionPhase::Opening,
            Self::Negotiating { .. } => ConnectionPhase::Negotiating,
            Self::Active {
                mode: ActiveMode::Ready,
                ..
            } => ConnectionPhase::Ready,
            Self::Active {
                mode: ActiveMode::Draining,
                ..
            } => ConnectionPhase::Draining,
            Self::Closing { .. } => ConnectionPhase::Closing,
            Self::Closed { .. } => ConnectionPhase::Closed,
        }
    }

    pub(super) fn snapshot(&self) -> ConnectionState {
        match self {
            Self::Dormant { epoch } => ConnectionState::Dormant { epoch: *epoch },
            Self::Opening {
                epoch,
                effect_id,
                transport_id,
            } => ConnectionState::Opening {
                epoch: *epoch,
                effect_id: *effect_id,
                transport_id: *transport_id,
            },
            Self::Negotiating {
                epoch,
                transport_id,
                effect_id,
                deadline_timer,
                deadline,
            } => ConnectionState::Negotiating {
                epoch: *epoch,
                transport_id: *transport_id,
                effect_id: *effect_id,
                deadline_timer: *deadline_timer,
                deadline: *deadline,
            },
            Self::Active { mode, connection } => match mode {
                ActiveMode::Ready => ConnectionState::Ready {
                    epoch: connection.epoch,
                    transport_id: connection.transport_id,
                    pending: connection.pending.len(),
                    capabilities: connection.capabilities.len(),
                },
                ActiveMode::Draining => ConnectionState::Draining {
                    epoch: connection.epoch,
                    transport_id: connection.transport_id,
                    pending: connection.pending.len(),
                    capabilities: connection.capabilities.len(),
                },
            },
            Self::Closing {
                epoch,
                transport_id,
                reason,
            } => ConnectionState::Closing {
                epoch: *epoch,
                transport_id: *transport_id,
                reason: *reason,
            },
            Self::Closed { epoch, reason } => ConnectionState::Closed {
                epoch: *epoch,
                reason: *reason,
            },
        }
    }
}
