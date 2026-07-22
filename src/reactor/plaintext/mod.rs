//! Plaintext nonblocking TCP driven through bounded sans-I/O primitives.

mod connection;
mod error;
mod limits;
mod progress;

#[cfg(test)]
mod connection_test;

pub(in crate::reactor) use connection::PlaintextConnection;
pub(in crate::reactor) use error::PlaintextError;
pub(in crate::reactor) use limits::{PlaintextLimits, ReadBudget, WriteBudget};
pub(in crate::reactor) use progress::{
    CompletedWrite, ReadProgress, ReadState, WriteDrive, WriteState,
};
