//! Exclusive ownership of the one authentication response currently expected.

use super::{AuthenticateExchange, HandshakeExchange};

/// Exactly one generated authentication exchange awaiting its FIFO response.
#[derive(Debug)]
pub(crate) enum AuthenticationExchange {
    Handshake(HandshakeExchange),
    Authenticate(AuthenticateExchange),
}
