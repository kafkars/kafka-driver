//! Ordered failure and cleanup effects when an active epoch must end.

use std::mem;

use crate::CallId;

use super::{CallFailure, CloseReason, ConnectionEffect, ConnectionMachine, StateData};

impl ConnectionMachine {
    pub(super) fn begin_active_close(
        &mut self,
        reason: CloseReason,
        specific_failure: Option<(CallId, CallFailure)>,
    ) -> Vec<ConnectionEffect> {
        let epoch = self.state.epoch();
        let placeholder = StateData::Closed { epoch, reason };
        let previous = mem::replace(&mut self.state, placeholder);
        let StateData::Active { mut connection, .. } = previous else {
            return Vec::new();
        };
        let transport_id = connection.transport_id;
        let mut effects = Vec::with_capacity(1 + connection.pending.len() * 2);
        effects.push(ConnectionEffect::CloseTransport {
            epoch,
            transport_id,
            reason,
        });
        append_failures(&mut effects, &mut connection, reason, specific_failure);
        self.state = StateData::Closing {
            epoch,
            transport_id,
            reason,
        };
        effects
    }

    pub(super) fn finish_active_close(
        &mut self,
        reason: CloseReason,
        specific_failure: Option<(CallId, CallFailure)>,
    ) -> Vec<ConnectionEffect> {
        let epoch = self.state.epoch();
        let placeholder = StateData::Closed { epoch, reason };
        let previous = mem::replace(&mut self.state, placeholder);
        let StateData::Active { mut connection, .. } = previous else {
            return Vec::new();
        };
        let mut effects = Vec::with_capacity(connection.pending.len() * 2);
        append_failures(&mut effects, &mut connection, reason, specific_failure);
        effects
    }
}

fn append_failures(
    effects: &mut Vec<ConnectionEffect>,
    connection: &mut super::ActiveConnection,
    reason: CloseReason,
    specific_failure: Option<(CallId, CallFailure)>,
) {
    for pending in connection.pending.drain() {
        let failure = specific_failure
            .filter(|(call_id, _)| *call_id == pending.call_id())
            .map_or(CallFailure::ConnectionClosed { reason }, |(_, failure)| {
                failure
            });
        effects.push(ConnectionEffect::CancelDeadline {
            timer_id: pending.deadline_timer(),
        });
        effects.push(ConnectionEffect::FailCall {
            call_id: pending.call_id(),
            failure,
            delivery: pending.delivery(),
        });
    }
}
