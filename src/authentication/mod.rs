//! Reactor-side SASL mechanism sessions that exclusively own credential bytes.

mod error;
mod exchange;
mod handshake;
mod pending;
mod plain;
mod scram;
mod session;
mod start_error;

#[cfg(test)]
mod framing_test;
#[cfg(test)]
mod plain_test;
#[cfg(test)]
mod response_bytes_test;

pub(crate) use error::AuthenticationExchangeError;
pub(crate) use exchange::AuthenticateExchange;
pub(crate) use handshake::{HandshakeExchange, HandshakeOutcome};
pub(crate) use pending::AuthenticationExchange;
pub(crate) use session::{AuthenticationReceive, AuthenticationSession};
pub(crate) use start_error::AuthenticationSessionStartError;

use plain::PlainSession;
use scram::{ScramReceive, ScramSession};
