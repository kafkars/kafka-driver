//! Explicit two-round SCRAM transcript state with terminal proof validation.

use std::{fmt, mem};

use kafka_driver_core::{AuthenticationFailure, ExchangeOutcome};
use zeroize::Zeroizing;

use crate::SaslConfig;

use super::{
    algorithm::ScramAlgorithm,
    client_first::client_first,
    limits::ScramLimits,
    message::{ServerFinal, parse_server_final, parse_server_first},
    nonce::ScramNonce,
    proof::derive_proof,
};

/// Secret-owning SCRAM exchange state for one connection epoch.
pub(crate) struct ScramSession {
    algorithm: ScramAlgorithm,
    limits: ScramLimits,
    state: ScramState,
}

enum ScramState {
    Ready {
        config: SaslConfig,
        nonce: ScramNonce,
    },
    AwaitingServerFirst {
        config: SaslConfig,
        nonce: ScramNonce,
        client_first_bare: Zeroizing<Vec<u8>>,
    },
    FinalReady {
        message: Zeroizing<Vec<u8>>,
        server_key: Zeroizing<Vec<u8>>,
        auth_message: Zeroizing<Vec<u8>>,
    },
    AwaitingServerFinal {
        server_key: Zeroizing<Vec<u8>>,
        auth_message: Zeroizing<Vec<u8>>,
    },
    Complete,
}

impl ScramSession {
    pub(crate) fn new(config: SaslConfig) -> Result<Self, AuthenticationFailure> {
        let limits = ScramLimits::default();
        let nonce = ScramNonce::generate(limits)?;
        Self::with_nonce_and_limits(config, nonce, limits)
    }

    #[cfg(test)]
    pub(crate) fn new_with_nonce(
        config: SaslConfig,
        nonce: impl Into<String>,
    ) -> Result<Self, AuthenticationFailure> {
        let limits = ScramLimits::default();
        let nonce = ScramNonce::new(nonce, limits)?;
        Self::with_nonce_and_limits(config, nonce, limits)
    }

    fn with_nonce_and_limits(
        config: SaslConfig,
        nonce: ScramNonce,
        limits: ScramLimits,
    ) -> Result<Self, AuthenticationFailure> {
        let algorithm = ScramAlgorithm::for_mechanism(config.mechanism())?;
        Ok(Self {
            algorithm,
            limits,
            state: ScramState::Ready { config, nonce },
        })
    }

    pub(crate) fn next_message(
        &mut self,
        max_bytes: usize,
    ) -> Result<Zeroizing<Vec<u8>>, AuthenticationFailure> {
        let state = mem::replace(&mut self.state, ScramState::Complete);
        match state {
            ScramState::Ready { config, nonce } => {
                match client_first(config.username(), &nonce, max_bytes) {
                    Ok(client_first) => {
                        self.state = ScramState::AwaitingServerFirst {
                            config,
                            nonce,
                            client_first_bare: client_first.bare,
                        };
                        Ok(client_first.message)
                    }
                    Err(failure) => {
                        self.state = ScramState::Ready { config, nonce };
                        Err(failure)
                    }
                }
            }
            ScramState::FinalReady {
                message,
                server_key,
                auth_message,
            } => {
                if message.len() > max_bytes {
                    self.state = ScramState::FinalReady {
                        message,
                        server_key,
                        auth_message,
                    };
                    return Err(AuthenticationFailure::Capacity);
                }
                self.state = ScramState::AwaitingServerFinal {
                    server_key,
                    auth_message,
                };
                Ok(message)
            }
            other => {
                self.state = other;
                Err(AuthenticationFailure::Protocol)
            }
        }
    }

    pub(crate) fn receive(&mut self, response: &[u8]) -> ExchangeOutcome {
        let state = mem::replace(&mut self.state, ScramState::Complete);
        match state {
            ScramState::AwaitingServerFirst {
                config,
                nonce,
                client_first_bare,
            } => self.receive_server_first(&config, &nonce, &client_first_bare, response),
            ScramState::AwaitingServerFinal {
                server_key,
                auth_message,
            } => self.receive_server_final(&server_key, &auth_message, response),
            other => {
                self.state = other;
                ExchangeOutcome::Failed(AuthenticationFailure::Protocol)
            }
        }
    }

    pub(crate) const fn proof_required(&self) -> bool {
        matches!(self.state, ScramState::AwaitingServerFirst { .. })
    }

    fn receive_server_first(
        &mut self,
        config: &SaslConfig,
        nonce: &ScramNonce,
        client_first_bare: &[u8],
        response: &[u8],
    ) -> ExchangeOutcome {
        let result = parse_server_first(response, nonce, self.limits).map(|server| {
            let client_final_without_proof =
                Zeroizing::new(format!("c=biws,r={}", server.nonce).into_bytes());
            let proof = derive_proof(
                self.algorithm,
                config.password(),
                &server.salt,
                server.iterations,
                client_first_bare,
                server.raw,
                &client_final_without_proof,
            );
            self.state = ScramState::FinalReady {
                message: proof.client_final,
                server_key: proof.server_key,
                auth_message: proof.auth_message,
            };
        });
        match result {
            Ok(()) => ExchangeOutcome::Continue,
            Err(failure) => ExchangeOutcome::Failed(failure),
        }
    }

    fn receive_server_final(
        &self,
        server_key: &[u8],
        auth_message: &[u8],
        response: &[u8],
    ) -> ExchangeOutcome {
        let result =
            parse_server_final(response, self.algorithm.output_len()).and_then(
                |final_| match final_ {
                    ServerFinal::Rejected => Err(AuthenticationFailure::Rejected),
                    ServerFinal::Verifier(signature) => {
                        self.algorithm.verify(server_key, auth_message, &signature)
                    }
                },
            );
        match result {
            Ok(()) => ExchangeOutcome::Succeeded,
            Err(failure) => ExchangeOutcome::Failed(failure),
        }
    }
}

impl fmt::Debug for ScramSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScramSession")
            .field("mechanism", &self.algorithm.mechanism())
            .field("phase", &self.state.name())
            .finish_non_exhaustive()
    }
}

impl ScramState {
    const fn name(&self) -> &'static str {
        match self {
            Self::Ready { .. } => "ready",
            Self::AwaitingServerFirst { .. } => "awaiting-server-first",
            Self::FinalReady { .. } => "final-ready",
            Self::AwaitingServerFinal { .. } => "awaiting-server-final",
            Self::Complete => "complete",
        }
    }
}
