//! Mechanism-polymorphic ownership of one connection epoch's secret transcript.

use kafka_driver_core::{AuthenticationFailure, ExchangeOutcome, SaslMechanism};
use zeroize::Zeroizing;

use crate::SaslConfig;

use super::{PlainSession, ScramSession};

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
                ScramSession::new(config).map(Self::Scram)
            }
        }
    }

    pub(crate) fn next_message(
        &mut self,
        max_bytes: usize,
    ) -> Result<Zeroizing<Vec<u8>>, AuthenticationFailure> {
        match self {
            Self::Plain(session) => session.next_message(max_bytes),
            Self::Scram(session) => session.next_message(max_bytes),
        }
    }

    pub(crate) fn receive(&mut self, response: &[u8]) -> ExchangeOutcome {
        match self {
            Self::Plain(session) => session.receive(response),
            Self::Scram(session) => session.receive(response),
        }
    }
}
