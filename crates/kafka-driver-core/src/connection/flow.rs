//! Admission, write, FIFO response, and deadline transitions for active calls.

use crate::{CallId, ConnectionEpoch, Delivery, EffectId, Moment, TimerId, TransportId};

use super::{
    ActiveMode, CallFailure, CloseReason, ConnectionEffect, ConnectionMachine,
    ConnectionMachineError, Decision, IdentityKind, PendingCall, PendingPhase, StateData,
    TransportFailure,
};

impl ConnectionMachine {
    pub(super) fn submit(
        &mut self,
        call_id: CallId,
        write_effect: EffectId,
        deadline_timer: TimerId,
        now: Moment,
        deadline: Moment,
    ) -> Result<Decision, ConnectionMachineError> {
        let StateData::Active {
            mode: ActiveMode::Ready,
            connection,
        } = &mut self.state
        else {
            let failure = self.failure_for_closed_state();
            return Ok(reject(call_id, failure));
        };
        if deadline <= now {
            return Ok(reject(call_id, CallFailure::DeadlineExceeded));
        }
        if connection.pending.is_full() {
            return Ok(reject(
                call_id,
                CallFailure::CapacityReached {
                    limit: self.limits.max_in_flight().get(),
                },
            ));
        }
        let conflicts = connection
            .pending
            .has_identities(call_id, write_effect, deadline_timer);
        if conflicts.call {
            return Err(ConnectionMachineError::IdentityInUse(IdentityKind::Call));
        }
        if conflicts.effect {
            return Err(ConnectionMachineError::IdentityInUse(
                IdentityKind::WriteEffect,
            ));
        }
        if conflicts.timer {
            return Err(ConnectionMachineError::IdentityInUse(
                IdentityKind::DeadlineTimer,
            ));
        }
        let pending = &connection.pending;
        let Some(correlation_id) = connection
            .correlations
            .allocate(pending.len(), |candidate| {
                pending.contains_correlation(candidate)
            })
        else {
            return Ok(reject(call_id, CallFailure::CorrelationSpaceExhausted));
        };
        connection.pending.push(PendingCall::new(
            call_id,
            correlation_id,
            write_effect,
            deadline_timer,
            deadline,
        ));
        Ok(Decision::applied(vec![
            ConnectionEffect::ScheduleDeadline {
                epoch: connection.epoch,
                call_id,
                timer_id: deadline_timer,
                at: deadline,
            },
            ConnectionEffect::WriteRequest {
                epoch: connection.epoch,
                transport_id: connection.transport_id,
                call_id,
                correlation_id,
                effect_id: write_effect,
            },
        ]))
    }

    pub(super) fn write_submitted(
        &mut self,
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        effect_id: EffectId,
    ) -> Decision {
        let StateData::Active { connection, .. } = &mut self.state else {
            return Decision::stale();
        };
        if epoch != connection.epoch || transport_id != connection.transport_id {
            return Decision::stale();
        }
        let Some(pending) = connection.pending.by_write_effect_mut(effect_id) else {
            return Decision::stale();
        };
        if pending.phase() == PendingPhase::AwaitingResponse {
            return Decision::stale();
        }
        pending.mark_submitted();
        Decision::applied(Vec::new())
    }

    pub(super) fn abort_unsent_call(
        &mut self,
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        call_id: CallId,
        effect_id: EffectId,
    ) -> Decision {
        let StateData::Active { mode, connection } = &mut self.state else {
            return Decision::stale();
        };
        if epoch != connection.epoch || transport_id != connection.transport_id {
            return Decision::stale();
        }
        let Some(aborted) = connection.pending.remove_awaiting_write(call_id, effect_id) else {
            return Decision::stale();
        };
        let mut effects = vec![
            ConnectionEffect::CancelDeadline {
                timer_id: aborted.deadline_timer(),
            },
            ConnectionEffect::FailCall {
                call_id,
                failure: CallFailure::LocallyRejected,
                delivery: Delivery::NotSent,
            },
        ];
        if *mode == ActiveMode::Draining && connection.pending.is_empty() {
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

    pub(super) fn write_failed(
        &mut self,
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        effect_id: EffectId,
        failure: TransportFailure,
    ) -> Decision {
        let StateData::Active { connection, .. } = &self.state else {
            return Decision::stale();
        };
        if epoch != connection.epoch
            || transport_id != connection.transport_id
            || connection.pending.iter().all(|pending| {
                pending.write_effect() != effect_id
                    || pending.phase() != PendingPhase::AwaitingWrite
            })
        {
            return Decision::stale();
        }
        let reason = CloseReason::TransportLost(failure);
        let effects = self.begin_active_close(reason, None);
        Decision::applied(effects)
    }
}

fn reject(call_id: CallId, failure: CallFailure) -> Decision {
    Decision::rejected(vec![ConnectionEffect::FailCall {
        call_id,
        failure,
        delivery: Delivery::NotSent,
    }])
}
