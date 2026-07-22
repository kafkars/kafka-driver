//! Reactor-side SASL mechanism sessions that exclusively own credential bytes.

mod error;
mod exchange;
mod handshake;
mod pending;
mod plain;
mod session;

#[cfg(test)]
mod plain_test;

pub(crate) use error::AuthenticationExchangeError;
pub(crate) use exchange::AuthenticateExchange;
pub(crate) use handshake::{HandshakeExchange, HandshakeOutcome};
pub(crate) use pending::AuthenticationExchange;
pub(crate) use session::AuthenticationSession;

use plain::PlainSession;
