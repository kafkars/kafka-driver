//! Drain initiation and terminal lifecycle transitions.

use crate::AuthenticationEffect;

use super::{
    ActiveMode, CallFailure, CloseReason, ConnectionEffect, ConnectionMachine, Decision, StateData,
};

impl ConnectionMachine {
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
                deadline_timer,
                ..
            }
            | StateData::Negotiating {
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
            StateData::Closing { reason, .. } | StateData::Closed { reason, .. } => {
                CallFailure::ConnectionClosed { reason: *reason }
            }
        }
    }
}
