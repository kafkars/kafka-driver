//! Bounded SASL handshake and exchange transitions for one Kafka session.

use std::mem;

use crate::{AuthenticationFailure, AuthenticationRound, ExchangeOutcome};

use super::{AuthenticationStage, Decision, KafkaSessionEffect, KafkaSessionMachine, StateData};

impl KafkaSessionMachine {
    pub(super) fn authentication_handshake_succeeded(&mut self) -> Decision {
        let StateData::Authenticating {
            protocol,
            stage: AuthenticationStage::Handshaking { deadline },
            ..
        } = &mut self.state
        else {
            return Decision::stale();
        };
        let round = AuthenticationRound::FIRST;
        let deadline = *deadline;
        let version = protocol.authenticate_version();
        *self.authentication_stage_mut() = AuthenticationStage::Exchanging { round, deadline };
        Decision::applied(vec![KafkaSessionEffect::StartAuthenticationExchange {
            round,
            version,
            deadline,
        }])
    }

    pub(super) fn authentication_exchange_completed(
        &mut self,
        round: AuthenticationRound,
        outcome: ExchangeOutcome,
    ) -> Decision {
        let StateData::Authenticating {
            protocol,
            stage:
                AuthenticationStage::Exchanging {
                    round: expected,
                    deadline,
                },
            ..
        } = &self.state
        else {
            return Decision::stale();
        };
        if round != *expected {
            return Decision::stale();
        }
        let deadline = *deadline;
        let version = protocol.authenticate_version();
        match outcome {
            ExchangeOutcome::Succeeded => self.finish_authentication(),
            ExchangeOutcome::Failed(failure) => self.authentication_failed(failure),
            ExchangeOutcome::Continue => {
                let Some(next) = round.next() else {
                    return self.authentication_failed(AuthenticationFailure::TooManyRounds);
                };
                let Some(policy) = self.authentication else {
                    return self.authentication_failed(AuthenticationFailure::Protocol);
                };
                if next.get() > policy.limits().max_exchange_rounds().get() {
                    return self.authentication_failed(AuthenticationFailure::TooManyRounds);
                }
                *self.authentication_stage_mut() = AuthenticationStage::Exchanging {
                    round: next,
                    deadline,
                };
                Decision::applied(vec![KafkaSessionEffect::StartAuthenticationExchange {
                    round: next,
                    version,
                    deadline,
                }])
            }
        }
    }

    pub(super) fn authentication_failed(&mut self, failure: AuthenticationFailure) -> Decision {
        if !matches!(self.state, StateData::Authenticating { .. }) {
            return Decision::stale();
        }
        self.close_authentication(failure, true)
    }

    fn finish_authentication(&mut self) -> Decision {
        let placeholder = StateData::Closing {
            reason: super::KafkaSessionCloseReason::Requested,
        };
        let previous = mem::replace(&mut self.state, placeholder);
        let StateData::Authenticating { capabilities, .. } = previous else {
            return Decision::stale();
        };
        self.state = StateData::Ready { capabilities };
        Decision::applied(vec![
            KafkaSessionEffect::CancelDeadline,
            KafkaSessionEffect::SessionReady,
        ])
    }

    fn authentication_stage_mut(&mut self) -> &mut AuthenticationStage {
        let StateData::Authenticating { stage, .. } = &mut self.state else {
            unreachable!("authentication stage is guarded before mutation");
        };
        stage
    }
}
