//! Bounded mechanism challenge-response round transitions.

use crate::{ConnectionEpoch, EffectId, TransportId};

use super::{
    AuthenticationFailure, AuthenticationMachine, AuthenticationRound, Decision, ExchangeOutcome,
    StateData,
};

impl AuthenticationMachine {
    pub(super) fn exchange_completed(
        &mut self,
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        effect_id: EffectId,
        round: AuthenticationRound,
        outcome: ExchangeOutcome,
    ) -> Decision {
        let StateData::Exchanging {
            effect_id: expected_effect,
            round: expected_round,
            deadline_timer,
            deadline,
        } = self.state
        else {
            return Decision::stale();
        };
        if !self.matches(epoch, transport_id, effect_id, expected_effect) || round != expected_round
        {
            return Decision::stale();
        }
        match outcome {
            ExchangeOutcome::Succeeded => self.succeed(deadline_timer),
            ExchangeOutcome::Failed(failure) => self.fail(failure, Some(deadline_timer)),
            ExchangeOutcome::Continue => {
                let Some(next) = round.next() else {
                    return self.fail(AuthenticationFailure::TooManyRounds, Some(deadline_timer));
                };
                if next.get() > self.limits.max_exchange_rounds().get() {
                    return self.fail(AuthenticationFailure::TooManyRounds, Some(deadline_timer));
                }
                self.state = StateData::Exchanging {
                    effect_id,
                    round: next,
                    deadline_timer,
                    deadline,
                };
                Decision::applied(vec![self.exchange_effect(effect_id, next)])
            }
        }
    }
}
