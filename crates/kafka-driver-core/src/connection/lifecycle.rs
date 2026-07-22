//! Open, drain, external-close, and terminal lifecycle transitions.

use crate::{AuthenticationEffect, ConnectionEpoch, EffectId, TransportId};

use super::{
    ActiveMode, CallFailure, CloseReason, ConnectionEffect, ConnectionMachine, Decision,
    NegotiationAttempt, StateData, TransportFailure,
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
        negotiation: NegotiationAttempt,
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
        self.begin_negotiation(epoch, transport_id, negotiation)
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
            StateData::Negotiating {
                epoch,
                transport_id,
                deadline_timer,
                ..
            } => {
                let epoch = *epoch;
                let transport_id = *transport_id;
                let deadline_timer = *deadline_timer;
                self.state = StateData::Closing {
                    epoch,
                    transport_id,
                    reason: CloseReason::Requested,
                };
                Decision::applied(vec![
                    ConnectionEffect::CloseTransport {
                        epoch,
                        transport_id,
                        reason: CloseReason::Requested,
                    },
                    ConnectionEffect::CancelDeadline {
                        timer_id: deadline_timer,
                    },
                ])
            }
            StateData::Authenticating { .. } => self.begin_authentication_drain(),
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

    fn begin_authentication_drain(&mut self) -> Decision {
        let StateData::Authenticating {
            epoch,
            transport_id,
            authentication,
            ..
        } = &self.state
        else {
            return Decision::stale();
        };
        let epoch = *epoch;
        let transport_id = *transport_id;
        let deadline_timer = authentication.deadline_timer();
        self.state = StateData::Closing {
            epoch,
            transport_id,
            reason: CloseReason::Requested,
        };
        let mut effects = vec![ConnectionEffect::CloseTransport {
            epoch,
            transport_id,
            reason: CloseReason::Requested,
        }];
        if let Some(timer_id) = deadline_timer {
            effects.push(ConnectionEffect::Authentication {
                effect: AuthenticationEffect::CancelDeadline { timer_id },
            });
        }
        Decision::applied(effects)
    }

    pub(super) fn failure_for_closed_state(&self) -> CallFailure {
        match &self.state {
            StateData::Active {
                mode: ActiveMode::Draining,
                ..
            } => CallFailure::Draining,
            StateData::Dormant { .. }
            | StateData::Opening { .. }
            | StateData::Negotiating { .. }
            | StateData::Authenticating { .. }
            | StateData::Active {
                mode: ActiveMode::Ready,
                ..
            } => CallFailure::NotReady,
            StateData::Closing { .. } | StateData::Closed { .. } => CallFailure::Closed,
        }
    }
}
