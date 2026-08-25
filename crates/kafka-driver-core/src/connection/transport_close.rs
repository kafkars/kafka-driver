//! External transport-close outcomes and their phase-specific cleanup.

use crate::{AuthenticationEffect, ConnectionEpoch, TransportId};

use super::{
    CloseReason, ConnectionEffect, ConnectionMachine, Decision, StateData, TransportFailure,
};

impl ConnectionMachine {
    pub(super) fn transport_closed(
        &mut self,
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        failure: TransportFailure,
    ) -> Decision {
        if epoch != self.session.state.epoch() {
            return Decision::stale();
        }
        match &self.session.state {
            StateData::Opening {
                transport_id: expected,
                deadline_timer,
                ..
            } if *expected == transport_id => {
                let deadline_timer = *deadline_timer;
                self.session.state = StateData::Closed {
                    epoch,
                    reason: CloseReason::TransportLost(failure),
                };
                Decision::applied(vec![ConnectionEffect::CancelDeadline {
                    timer_id: deadline_timer,
                }])
            }
            StateData::Negotiating {
                transport_id: expected,
                deadline_timer,
                ..
            } if *expected == transport_id => {
                let deadline_timer = *deadline_timer;
                self.session.state = StateData::Closed {
                    epoch,
                    reason: CloseReason::TransportLost(failure),
                };
                Decision::applied(vec![ConnectionEffect::CancelDeadline {
                    timer_id: deadline_timer,
                }])
            }
            StateData::Authenticating {
                transport_id: expected,
                authentication,
                ..
            } if *expected == transport_id => {
                let deadline_timer = authentication.deadline_timer();
                self.session.state = StateData::Closed {
                    epoch,
                    reason: CloseReason::TransportLost(failure),
                };
                let effects =
                    deadline_timer
                        .into_iter()
                        .map(|timer_id| ConnectionEffect::Authentication {
                            effect: AuthenticationEffect::CancelDeadline { timer_id },
                        });
                Decision::applied(effects.collect())
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
                self.session.state = StateData::Closed { epoch, reason };
                Decision::applied(Vec::new())
            }
            StateData::Dormant { .. }
            | StateData::Opening { .. }
            | StateData::Negotiating { .. }
            | StateData::Authenticating { .. }
            | StateData::Active { .. }
            | StateData::Closing { .. }
            | StateData::Closed { .. } => Decision::stale(),
        }
    }
}
