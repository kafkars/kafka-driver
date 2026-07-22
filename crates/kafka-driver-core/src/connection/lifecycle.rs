//! Open, drain, external-close, and terminal lifecycle transitions.

use crate::{ConnectionEpoch, EffectId, TransportId};

use super::{
    ActiveConnection, ActiveMode, CallFailure, CloseReason, ConnectionEffect, ConnectionMachine,
    Decision, StateData, TransportFailure,
};

impl ConnectionMachine {
    pub(super) fn start(&mut self, effect_id: EffectId, transport_id: TransportId) -> Decision {
        let StateData::Dormant { epoch } = self.state else {
            return Decision::ignored();
        };
        self.state = StateData::Opening {
            epoch,
            effect_id,
            transport_id,
        };
        Decision::applied(vec![ConnectionEffect::OpenTransport {
            epoch,
            effect_id,
            transport_id,
        }])
    }

    pub(super) fn transport_opened(
        &mut self,
        epoch: ConnectionEpoch,
        effect_id: EffectId,
        transport_id: TransportId,
    ) -> Decision {
        let StateData::Opening {
            epoch: expected_epoch,
            effect_id: expected_effect,
            transport_id: expected_transport,
        } = self.state
        else {
            return Decision::stale();
        };
        if epoch != expected_epoch
            || effect_id != expected_effect
            || transport_id != expected_transport
        {
            return Decision::stale();
        }
        self.state = StateData::Active {
            mode: ActiveMode::Ready,
            connection: ActiveConnection::new(epoch, transport_id, self.limits),
        };
        Decision::applied(Vec::new())
    }

    pub(super) fn transport_open_failed(
        &mut self,
        epoch: ConnectionEpoch,
        effect_id: EffectId,
        transport_id: TransportId,
        failure: TransportFailure,
    ) -> Decision {
        let StateData::Opening {
            epoch: expected_epoch,
            effect_id: expected_effect,
            transport_id: expected_transport,
        } = self.state
        else {
            return Decision::stale();
        };
        if epoch != expected_epoch
            || effect_id != expected_effect
            || transport_id != expected_transport
        {
            return Decision::stale();
        }
        self.state = StateData::Closed {
            epoch,
            reason: CloseReason::OpenFailed(failure),
        };
        Decision::applied(Vec::new())
    }

    pub(super) fn begin_drain(&mut self) -> Decision {
        match &mut self.state {
            StateData::Dormant { epoch } => {
                self.state = StateData::Closed {
                    epoch: *epoch,
                    reason: CloseReason::Requested,
                };
                Decision::applied(Vec::new())
            }
            StateData::Opening {
                epoch,
                transport_id,
                ..
            } => {
                let epoch = *epoch;
                let transport_id = *transport_id;
                self.state = StateData::Closing {
                    epoch,
                    transport_id,
                    reason: CloseReason::Requested,
                };
                Decision::applied(vec![ConnectionEffect::CloseTransport {
                    epoch,
                    transport_id,
                    reason: CloseReason::Requested,
                }])
            }
            StateData::Active {
                mode: ActiveMode::Ready,
                connection,
            } if connection.pending.is_empty() => {
                let epoch = connection.epoch;
                let transport_id = connection.transport_id;
                self.state = StateData::Closing {
                    epoch,
                    transport_id,
                    reason: CloseReason::Drained,
                };
                Decision::applied(vec![ConnectionEffect::CloseTransport {
                    epoch,
                    transport_id,
                    reason: CloseReason::Drained,
                }])
            }
            StateData::Active { mode, .. } if *mode == ActiveMode::Ready => {
                *mode = ActiveMode::Draining;
                Decision::applied(Vec::new())
            }
            StateData::Active { .. } | StateData::Closing { .. } | StateData::Closed { .. } => {
                Decision::ignored()
            }
        }
    }

    pub(super) fn transport_closed(
        &mut self,
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        failure: TransportFailure,
    ) -> Decision {
        if epoch != self.state.epoch() {
            return Decision::stale();
        }
        match &self.state {
            StateData::Opening {
                transport_id: expected,
                ..
            } if *expected == transport_id => {
                self.state = StateData::Closed {
                    epoch,
                    reason: CloseReason::TransportLost(failure),
                };
                Decision::applied(Vec::new())
            }
            StateData::Active { connection, .. } if connection.transport_id == transport_id => {
                let reason = CloseReason::TransportLost(failure);
                let effects = self.finish_active_close(reason, None);
                Decision::applied(effects)
            }
            StateData::Closing {
                transport_id: expected,
                reason,
                ..
            } if *expected == transport_id => {
                let reason = *reason;
                self.state = StateData::Closed { epoch, reason };
                Decision::applied(Vec::new())
            }
            StateData::Dormant { .. }
            | StateData::Opening { .. }
            | StateData::Active { .. }
            | StateData::Closing { .. }
            | StateData::Closed { .. } => Decision::stale(),
        }
    }

    pub(super) const fn failure_for_closed_state(&self) -> CallFailure {
        match self.state {
            StateData::Active {
                mode: ActiveMode::Draining,
                ..
            } => CallFailure::Draining,
            StateData::Dormant { .. }
            | StateData::Opening { .. }
            | StateData::Active {
                mode: ActiveMode::Ready,
                ..
            } => CallFailure::NotReady,
            StateData::Closing { .. } | StateData::Closed { .. } => CallFailure::Closed,
        }
    }
}
