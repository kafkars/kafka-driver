//! FIFO response verification, completion, draining, and deadline expiry.

use crate::{AuthenticationInput, ConnectionEpoch, Moment, TimerId, TransportId};

use super::{
    ActiveMode, CallFailure, CloseReason, ConnectionEffect, ConnectionMachine, CorrelationId,
    Decision, NegotiationFailure, PendingPhase, ResponseFault, StateData,
};

impl ConnectionMachine {
    pub(super) fn response_rejected(
        &mut self,
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        fault: ResponseFault,
    ) -> Decision {
        let StateData::Active { connection, .. } = &self.state else {
            return Decision::stale();
        };
        if epoch != connection.epoch || transport_id != connection.transport_id {
            return Decision::stale();
        }
        let reason = match fault {
            ResponseFault::Unexpected => CloseReason::UnexpectedResponse,
            ResponseFault::Malformed => CloseReason::MalformedResponse,
        };
        let effects = self.begin_active_close(reason, None);
        Decision::fault(effects)
    }

    pub(super) fn response_received(
        &mut self,
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        received: CorrelationId,
    ) -> Decision {
        let StateData::Active { connection, .. } = &self.state else {
            return Decision::stale();
        };
        if epoch != connection.epoch || transport_id != connection.transport_id {
            return Decision::stale();
        }
        let Some(front) = connection.pending.front().copied() else {
            let effects = self.begin_active_close(CloseReason::UnexpectedResponse, None);
            return Decision::fault(effects);
        };
        if front.phase() != PendingPhase::AwaitingResponse {
            let effects = self.begin_active_close(CloseReason::UnexpectedResponse, None);
            return Decision::fault(effects);
        }
        if front.correlation_id() != received {
            let reason = CloseReason::CorrelationMismatch {
                expected: front.correlation_id(),
                received,
            };
            let failure = CallFailure::CorrelationMismatch {
                expected: front.correlation_id(),
                received,
            };
            let effects = self.begin_active_close(reason, Some((front.call_id(), failure)));
            return Decision::fault(effects);
        }

        let StateData::Active { mode, connection } = &mut self.state else {
            return Decision::stale();
        };
        let Some(completed) = connection.pending.pop_front() else {
            return Decision::stale();
        };
        let mut effects = vec![
            ConnectionEffect::CancelDeadline {
                timer_id: completed.deadline_timer(),
            },
            ConnectionEffect::CompleteResponse {
                call_id: completed.call_id(),
                correlation_id: completed.correlation_id(),
            },
        ];
        if *mode == ActiveMode::Draining && connection.pending.is_empty() {
            let epoch = connection.epoch;
            let transport_id = connection.transport_id;
            self.state = StateData::Closing {
                epoch,
                transport_id,
                reason: CloseReason::Drained,
            };
            effects.push(ConnectionEffect::CloseTransport {
                epoch,
                transport_id,
                reason: CloseReason::Drained,
            });
        }
        Decision::applied(effects)
    }

    pub(super) fn deadline_elapsed(
        &mut self,
        epoch: ConnectionEpoch,
        timer_id: TimerId,
        now: Moment,
    ) -> Decision {
        if let StateData::Opening {
            epoch: expected_epoch,
            transport_id,
            deadline_timer,
            deadline,
            ..
        } = self.state
        {
            if epoch != expected_epoch || timer_id != deadline_timer {
                return Decision::stale();
            }
            if now < deadline {
                return Decision::applied(vec![ConnectionEffect::ScheduleOpenDeadline {
                    epoch,
                    timer_id,
                    at: deadline,
                }]);
            }
            let reason = CloseReason::OpenFailed(super::TransportFailure::TimedOut);
            self.state = StateData::Closing {
                epoch,
                transport_id,
                reason,
            };
            return Decision::applied(vec![ConnectionEffect::CloseTransport {
                epoch,
                transport_id,
                reason,
            }]);
        }
        if matches!(&self.state, StateData::Authenticating { .. }) {
            return self.authentication_input(AuthenticationInput::DeadlineElapsed {
                epoch,
                timer_id,
                now,
            });
        }
        if let StateData::Negotiating {
            epoch: expected_epoch,
            transport_id,
            deadline_timer,
            deadline,
            ..
        } = self.state
        {
            if epoch != expected_epoch || timer_id != deadline_timer {
                return Decision::stale();
            }
            if now < deadline {
                return Decision::applied(vec![ConnectionEffect::ScheduleNegotiationDeadline {
                    epoch,
                    timer_id,
                    at: deadline,
                }]);
            }
            let reason = CloseReason::NegotiationFailed(NegotiationFailure::Timeout);
            self.state = StateData::Closing {
                epoch,
                transport_id,
                reason,
            };
            return Decision::applied(vec![ConnectionEffect::CloseTransport {
                epoch,
                transport_id,
                reason,
            }]);
        }
        let StateData::Active { connection, .. } = &self.state else {
            return Decision::stale();
        };
        if epoch != connection.epoch {
            return Decision::stale();
        }
        let Some(pending) = connection.pending.by_timer(timer_id).copied() else {
            return Decision::stale();
        };
        if now < pending.deadline() {
            return Decision::applied(vec![ConnectionEffect::ScheduleDeadline {
                epoch,
                call_id: pending.call_id(),
                timer_id,
                at: pending.deadline(),
            }]);
        }
        let reason = CloseReason::DeadlineExceeded {
            call_id: pending.call_id(),
        };
        let effects = self.begin_active_close(
            reason,
            Some((pending.call_id(), CallFailure::DeadlineExceeded)),
        );
        Decision::applied(effects)
    }
}
