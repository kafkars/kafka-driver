//! Bounded construction and terminal validation of one SASL PLAIN exchange.

use kafka_driver_core::{AuthenticationFailure, ExchangeOutcome, SaslMechanism};
use zeroize::Zeroizing;

use crate::SaslConfig;

/// Secret-owning SASL PLAIN exchange state for one connection epoch.
#[derive(Debug)]
pub(crate) struct PlainSession {
    state: PlainState,
}

#[derive(Debug)]
enum PlainState {
    Ready(SaslConfig),
    AwaitingResponse,
    Complete,
}

impl PlainSession {
    pub(crate) fn new(config: SaslConfig) -> Self {
        debug_assert_eq!(config.mechanism(), SaslMechanism::Plain);
        Self {
            state: PlainState::Ready(config),
        }
    }

    pub(crate) fn next_message(
        &mut self,
        max_bytes: usize,
    ) -> Result<Zeroizing<Vec<u8>>, AuthenticationFailure> {
        let PlainState::Ready(config) = &self.state else {
            return Err(AuthenticationFailure::Protocol);
        };
        let message_bytes = config
            .authorization_identity()
            .len()
            .checked_add(config.username().len())
            .and_then(|bytes| bytes.checked_add(config.password().len()))
            .and_then(|bytes| bytes.checked_add(2))
            .ok_or(AuthenticationFailure::PolicyLimitExceeded)?;
        if message_bytes > max_bytes {
            return Err(AuthenticationFailure::PolicyLimitExceeded);
        }
        let mut message = Zeroizing::new(Vec::with_capacity(message_bytes));
        message.extend_from_slice(config.authorization_identity().as_bytes());
        message.push(0);
        message.extend_from_slice(config.username().as_bytes());
        message.push(0);
        message.extend_from_slice(config.password().as_bytes());
        self.state = PlainState::AwaitingResponse;
        Ok(message)
    }

    pub(crate) fn receive(&mut self, response: &[u8]) -> ExchangeOutcome {
        if !matches!(self.state, PlainState::AwaitingResponse) {
            return ExchangeOutcome::Failed(AuthenticationFailure::Protocol);
        }
        self.state = PlainState::Complete;
        if response.is_empty() {
            ExchangeOutcome::Succeeded
        } else {
            ExchangeOutcome::Failed(AuthenticationFailure::Malformed)
        }
    }
}
