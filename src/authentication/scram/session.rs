//! Kafka-profile composition of consuming SCRAM client continuations.

use std::{fmt, mem};

use kafka_driver_core::{AuthenticationFailure, ExchangeOutcome, SaslMechanism};
use sasl_scram::{
    AwaitingServerFinal, AwaitingServerFirst, Client, Error, NonceSource, OutboundMessage,
    PendingDerivation,
};

use crate::{
    SaslConfig, authentication::AuthenticationSessionStartError, config::kafka_scram_client_config,
};

use super::{error::failure, nonce::SecureNonceSource};

pub(crate) struct ScramSession {
    mechanism: SaslMechanism,
    state: ScramState,
}

enum ScramState {
    ClientFirstReady {
        next: AwaitingServerFirst,
        message: OutboundMessage,
    },
    AwaitingServerFirst(AwaitingServerFirst),
    Deriving,
    ClientFinalReady {
        next: AwaitingServerFinal,
        message: OutboundMessage,
    },
    AwaitingServerFinal(AwaitingServerFinal),
    Complete,
}

pub(in crate::authentication) enum ScramReceive {
    Derive(PendingDerivation),
    Outcome(ExchangeOutcome),
}

impl ScramSession {
    pub(in crate::authentication) fn new(
        config: &SaslConfig,
    ) -> Result<Self, AuthenticationSessionStartError> {
        Self::with_nonce_source(config, &mut SecureNonceSource::new())
    }

    #[cfg(test)]
    pub(super) fn new_with_nonce_source(
        config: &SaslConfig,
        nonce: &mut impl NonceSource,
    ) -> Result<Self, AuthenticationSessionStartError> {
        Self::with_nonce_source(config, nonce)
    }

    fn with_nonce_source(
        config: &SaslConfig,
        nonce: &mut impl NonceSource,
    ) -> Result<Self, AuthenticationSessionStartError> {
        let mechanism = config.mechanism();
        let config =
            kafka_scram_client_config(mechanism, config.username(), config.password().as_bytes())
                .map_err(|_| AuthenticationSessionStartError::ScramConfigurationInvalid)?;
        let (next, message) = Client::start(config, nonce).map_err(start_error)?;
        Ok(Self {
            mechanism,
            state: ScramState::ClientFirstReady { next, message },
        })
    }

    pub(in crate::authentication) fn next_message(
        &mut self,
        max_bytes: usize,
    ) -> Result<OutboundMessage, AuthenticationFailure> {
        let state = mem::replace(&mut self.state, ScramState::Complete);
        match state {
            ScramState::ClientFirstReady { next, message } => {
                if message.len() > max_bytes {
                    self.state = ScramState::ClientFirstReady { next, message };
                    return Err(AuthenticationFailure::PolicyLimitExceeded);
                }
                self.state = ScramState::AwaitingServerFirst(next);
                Ok(message)
            }
            ScramState::ClientFinalReady { next, message } => {
                if message.len() > max_bytes {
                    self.state = ScramState::ClientFinalReady { next, message };
                    return Err(AuthenticationFailure::PolicyLimitExceeded);
                }
                self.state = ScramState::AwaitingServerFinal(next);
                Ok(message)
            }
            other => {
                self.state = other;
                Err(AuthenticationFailure::Protocol)
            }
        }
    }

    pub(in crate::authentication) fn receive(&mut self, response: &[u8]) -> ScramReceive {
        let state = mem::replace(&mut self.state, ScramState::Complete);
        match state {
            ScramState::AwaitingServerFirst(state) => match state.receive_server_first(response) {
                Ok(pending) => {
                    self.state = ScramState::Deriving;
                    ScramReceive::Derive(pending)
                }
                Err(error) => ScramReceive::Outcome(ExchangeOutcome::Failed(failure(error))),
            },
            ScramState::AwaitingServerFinal(state) => {
                let outcome = match state.receive_server_final(response) {
                    Ok(_) => ExchangeOutcome::Succeeded,
                    Err(error) => ExchangeOutcome::Failed(failure(error)),
                };
                ScramReceive::Outcome(outcome)
            }
            other => {
                self.state = other;
                ScramReceive::Outcome(ExchangeOutcome::Failed(AuthenticationFailure::Protocol))
            }
        }
    }

    pub(in crate::authentication) fn complete_derivation(
        &mut self,
        result: Result<(AwaitingServerFinal, OutboundMessage), Error>,
    ) -> ExchangeOutcome {
        let state = mem::replace(&mut self.state, ScramState::Complete);
        if !matches!(state, ScramState::Deriving) {
            self.state = state;
            return ExchangeOutcome::Failed(AuthenticationFailure::Protocol);
        }
        match result {
            Ok((next, message)) => {
                self.state = ScramState::ClientFinalReady { next, message };
                ExchangeOutcome::Continue
            }
            Err(error) => ExchangeOutcome::Failed(failure(error)),
        }
    }
}

fn start_error(error: Error) -> AuthenticationSessionStartError {
    match error {
        Error::Nonce(_) => AuthenticationSessionStartError::ScramNonceUnavailable,
        _ => AuthenticationSessionStartError::ScramConfigurationInvalid,
    }
}

impl fmt::Debug for ScramSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScramSession")
            .field("mechanism", &self.mechanism)
            .field("phase", &self.state.name())
            .finish_non_exhaustive()
    }
}

impl ScramState {
    const fn name(&self) -> &'static str {
        match self {
            Self::ClientFirstReady { .. } => "client-first-ready",
            Self::AwaitingServerFirst(_) => "awaiting-server-first",
            Self::Deriving => "deriving",
            Self::ClientFinalReady { .. } => "client-final-ready",
            Self::AwaitingServerFinal(_) => "awaiting-server-final",
            Self::Complete => "complete",
        }
    }
}
