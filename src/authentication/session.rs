//! Mechanism-polymorphic ownership of one connection epoch's secret transcript.

use kafka_driver_core::{AuthenticationFailure, ExchangeOutcome, SaslMechanism};
use sasl_scram::{Error, OutboundMessage, PendingDerivation};
use zeroize::Zeroizing;

use crate::SaslConfig;

use super::{PlainSession, ScramReceive, ScramSession};

/// Zeroizing ownership of one mechanism message until Kafka framing copies it.
#[derive(Debug)]
pub(crate) enum AuthenticationMessage {
    Plain(Zeroizing<Vec<u8>>),
    Scram(OutboundMessage),
}

impl AuthenticationMessage {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Plain(message) => message,
            Self::Scram(message) => message.as_bytes(),
        }
    }
}

/// Immediate mechanism progress or explicit off-reactor SCRAM derivation work.
pub(crate) enum AuthenticationReceive {
    Derive(PendingDerivation),
    Outcome(ExchangeOutcome),
}

/// Secret-owning mechanism session selected by public SASL configuration.
#[derive(Debug)]
pub(crate) enum AuthenticationSession {
    Plain(PlainSession),
    Scram(ScramSession),
}

impl AuthenticationSession {
    pub(crate) fn new(config: SaslConfig) -> Result<Self, AuthenticationFailure> {
        match config.mechanism() {
            SaslMechanism::Plain => PlainSession::new(config).map(Self::Plain),
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                ScramSession::new(&config).map(Self::Scram)
            }
        }
    }

    pub(crate) fn next_message(
        &mut self,
        max_bytes: usize,
    ) -> Result<AuthenticationMessage, AuthenticationFailure> {
        match self {
            Self::Plain(session) => session
                .next_message(max_bytes)
                .map(AuthenticationMessage::Plain),
            Self::Scram(session) => session
                .next_message(max_bytes)
                .map(AuthenticationMessage::Scram),
        }
    }

    pub(crate) fn receive(&mut self, response: &[u8]) -> AuthenticationReceive {
        match self {
            Self::Plain(session) => AuthenticationReceive::Outcome(session.receive(response)),
            Self::Scram(session) => match session.receive(response) {
                ScramReceive::Derive(pending) => AuthenticationReceive::Derive(pending),
                ScramReceive::Outcome(outcome) => AuthenticationReceive::Outcome(outcome),
            },
        }
    }

    pub(crate) fn complete_derivation(
        &mut self,
        result: Result<(sasl_scram::AwaitingServerFinal, sasl_scram::OutboundMessage), Error>,
    ) -> ExchangeOutcome {
        match self {
            Self::Plain(_) => ExchangeOutcome::Failed(AuthenticationFailure::Protocol),
            Self::Scram(session) => session.complete_derivation(result),
        }
    }
}
