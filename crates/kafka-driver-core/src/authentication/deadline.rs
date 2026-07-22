//! Authentication deadline fencing, early delivery, and timeout transition.

use crate::{ConnectionEpoch, Moment, TimerId};

use super::{
    AuthenticationEffect, AuthenticationFailure, AuthenticationMachine, Decision, StateData,
};

impl AuthenticationMachine {
    pub(super) fn deadline_elapsed(
        &mut self,
        epoch: ConnectionEpoch,
        timer_id: TimerId,
        now: Moment,
    ) -> Decision {
        let Some((expected_timer, deadline)) = self.active_deadline() else {
            return Decision::stale();
        };
        if epoch != self.epoch || timer_id != expected_timer {
            return Decision::stale();
        }
        if now < deadline {
            return Decision::applied(vec![AuthenticationEffect::ScheduleDeadline {
                epoch,
                timer_id,
                at: deadline,
            }]);
        }
        self.fail(AuthenticationFailure::Timeout, Some(timer_id))
    }

    fn active_deadline(&self) -> Option<(TimerId, Moment)> {
        match self.state {
            StateData::Handshaking {
                deadline_timer,
                deadline,
                ..
            }
            | StateData::Exchanging {
                deadline_timer,
                deadline,
                ..
            } => Some((deadline_timer, deadline)),
            StateData::Dormant | StateData::Succeeded | StateData::Failed { .. } => None,
        }
    }
}
