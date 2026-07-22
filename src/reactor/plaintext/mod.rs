//! Plaintext nonblocking TCP driven through bounded sans-I/O primitives.

mod connection;
mod error;

#[cfg(test)]
mod connection_test;

pub(in crate::reactor) use connection::PlaintextConnection;
pub(in crate::reactor) use error::PlaintextError;
