//! Authentication start and mechanism-handshake transitions.

use crate::{ConnectionEpoch, EffectId, TransportId};

use super::{
    AuthenticationAttempt, AuthenticationEffect, AuthenticationFailure, AuthenticationMachine,
    AuthenticationRound, Decision, StateData,
};

const HANDSHAKE_CORRELATION: crate::CorrelationId = crate::CorrelationId::from_raw(1);

impl AuthenticationMachine {
    pub(super) fn start(&mut self, attempt: AuthenticationAttempt) -> Decision {
        if self.state != StateData::Dormant {
            return Decision::stale();
        }
        if attempt.deadline <= attempt.now {
            return self.fail(AuthenticationFailure::Timeout, None);
        }
        self.state = StateData::Handshaking {
            effect_id: attempt.effect_id,
            deadline_timer: attempt.deadline_timer,
            deadline: attempt.deadline,
        };
        Decision::applied(vec![
            AuthenticationEffect::ScheduleDeadline {
                epoch: self.epoch,
                timer_id: attempt.deadline_timer,
                at: attempt.deadline,
            },
            AuthenticationEffect::SendHandshake {
                epoch: self.epoch,
                transport_id: self.transport_id,
                effect_id: attempt.effect_id,
                correlation_id: HANDSHAKE_CORRELATION,
                mechanism: self.protocol.mechanism(),
                version: self.protocol.handshake_version(),
            },
        ])
    }

    pub(super) fn handshake_accepted(
        &mut self,
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        effect_id: EffectId,
    ) -> Decision {
        let StateData::Handshaking {
            effect_id: expected_effect,
            deadline_timer,
            deadline,
        } = self.state
        else {
            return Decision::stale();
        };
        if !self.matches(epoch, transport_id, effect_id, expected_effect) {
            return Decision::stale();
        }
        let round = AuthenticationRound::FIRST;
        self.state = StateData::Exchanging {
            effect_id,
            round,
            deadline_timer,
            deadline,
        };
        Decision::applied(vec![self.exchange_effect(effect_id, round)])
    }

    pub(super) fn handshake_failed(
        &mut self,
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        effect_id: EffectId,
        failure: AuthenticationFailure,
    ) -> Decision {
        let StateData::Handshaking {
            effect_id: expected_effect,
            deadline_timer,
            ..
        } = self.state
        else {
            return Decision::stale();
        };
        if !self.matches(epoch, transport_id, effect_id, expected_effect) {
            return Decision::stale();
        }
        self.fail(failure, Some(deadline_timer))
    }
}
