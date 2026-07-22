//! Internal lifecycle ownership and projection into public secret-free snapshots.

use crate::{
    AuthenticationMachine, ConnectionEpoch, EffectId, Moment, NegotiatedCapabilities, TimerId,
    TransportId,
};

use super::{ActiveConnection, ActiveMode, CloseReason, ConnectionPhase, ConnectionState};

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
    Authenticating {
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        capabilities: NegotiatedCapabilities,
        authentication: AuthenticationMachine,
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
            | Self::Authenticating { epoch, .. }
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
            Self::Authenticating { .. } => ConnectionPhase::Authenticating,
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
            Self::Authenticating {
                epoch,
                transport_id,
                capabilities,
                authentication,
            } => ConnectionState::Authenticating {
                epoch: *epoch,
                transport_id: *transport_id,
                authentication: authentication.state(),
                capabilities: capabilities.len(),
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
