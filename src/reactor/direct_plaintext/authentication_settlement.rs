//! SASL reply decoding and sanitized session-machine outcome delivery.

use bornera::RegisteredTransport;
use bornera_core::OperationFailure;
use kafka_driver_core::{
    AuthenticationFailure, AuthenticationRound, ExchangeOutcome, KafkaSessionAuthenticationState,
    KafkaSessionInput, KafkaSessionState, Moment,
};

use crate::authentication::{AuthenticationExchange, AuthenticationReceive, HandshakeOutcome};

use super::owner::DirectOwner;

#[derive(Clone, Copy)]
pub(super) enum AuthenticationStageOwner {
    Handshake,
    Exchange(AuthenticationRound),
}

impl<T: RegisteredTransport> DirectOwner<T> {
    pub(super) fn settle_authentication_reply(
        &mut self,
        exchange: AuthenticationExchange,
        frame: crate::reactor::bornera::KafkaFrame,
        now: Moment,
    ) -> std::io::Result<()> {
        match exchange {
            AuthenticationExchange::Handshake(exchange) => {
                match exchange.finish_bytes(frame.into_bytes()) {
                    Ok(HandshakeOutcome::Accepted) => {
                        self.apply_session(KafkaSessionInput::AuthenticationHandshakeSucceeded, now)
                    }
                    Ok(HandshakeOutcome::Unsupported) => self.fail_authentication_stage(
                        AuthenticationStageOwner::Handshake,
                        AuthenticationFailure::UnsupportedMechanism,
                        now,
                    ),
                    Err(error) => self.fail_authentication_stage(
                        AuthenticationStageOwner::Handshake,
                        error.failure(),
                        now,
                    ),
                }
            }
            AuthenticationExchange::Authenticate(exchange) => {
                let effect_id = exchange.effect_id();
                let round = exchange.round();
                let response = match exchange.finish_bytes(frame.into_bytes()) {
                    Ok(response) => response,
                    Err(error) => {
                        return self.fail_authentication_stage(
                            AuthenticationStageOwner::Exchange(round),
                            error.failure(),
                            now,
                        );
                    }
                };
                if response.error_code != 0 {
                    return self.fail_authentication_stage(
                        AuthenticationStageOwner::Exchange(round),
                        AuthenticationFailure::Rejected,
                        now,
                    );
                }
                let Some(session) = self.authentication_session.as_mut() else {
                    return self.fail_authentication_stage(
                        AuthenticationStageOwner::Exchange(round),
                        AuthenticationFailure::Protocol,
                        now,
                    );
                };
                match session.receive(&response.auth_bytes) {
                    AuthenticationReceive::Outcome(outcome) => self.apply_session(
                        KafkaSessionInput::AuthenticationExchangeCompleted { round, outcome },
                        now,
                    ),
                    AuthenticationReceive::Derive(pending) => {
                        self.dispatch_scram_proof(effect_id, round, pending, now)
                    }
                }
            }
        }
    }

    pub(super) fn settle_authentication_failure(
        &mut self,
        exchange: Option<AuthenticationExchange>,
        failure: OperationFailure,
        now: Moment,
    ) -> std::io::Result<()> {
        if matches!(failure, OperationFailure::ConnectionClosed(_)) {
            return Ok(());
        }
        let translated = match failure {
            OperationFailure::DeadlineElapsed => AuthenticationFailure::Timeout,
            OperationFailure::MatchKeyMismatch { .. } => AuthenticationFailure::Malformed,
            _ => AuthenticationFailure::Protocol,
        };
        match exchange.map(authentication_stage) {
            Some(stage) => self.fail_authentication_stage(stage, translated, now),
            None => self.fail_active_authentication(translated, now),
        }
    }

    pub(super) fn fail_authentication_stage(
        &mut self,
        stage: AuthenticationStageOwner,
        failure: AuthenticationFailure,
        now: Moment,
    ) -> std::io::Result<()> {
        let input = match stage {
            AuthenticationStageOwner::Handshake => {
                KafkaSessionInput::AuthenticationHandshakeFailed { failure }
            }
            AuthenticationStageOwner::Exchange(round) => {
                KafkaSessionInput::AuthenticationExchangeCompleted {
                    round,
                    outcome: ExchangeOutcome::Failed(failure),
                }
            }
        };
        self.apply_session(input, now)
    }

    pub(super) fn fail_active_authentication(
        &mut self,
        failure: AuthenticationFailure,
        now: Moment,
    ) -> std::io::Result<()> {
        let stage = match self.session.state() {
            KafkaSessionState::Authenticating {
                authentication: KafkaSessionAuthenticationState::Handshaking { .. },
                ..
            } => AuthenticationStageOwner::Handshake,
            KafkaSessionState::Authenticating {
                authentication: KafkaSessionAuthenticationState::Exchanging { round, .. },
                ..
            } => AuthenticationStageOwner::Exchange(round),
            _ => return Ok(()),
        };
        self.fail_authentication_stage(stage, failure, now)
    }
}

fn authentication_stage(exchange: AuthenticationExchange) -> AuthenticationStageOwner {
    match exchange {
        AuthenticationExchange::Handshake(_) => AuthenticationStageOwner::Handshake,
        AuthenticationExchange::Authenticate(exchange) => {
            AuthenticationStageOwner::Exchange(exchange.round())
        }
    }
}
