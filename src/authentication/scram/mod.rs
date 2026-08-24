//! Kafka-profile adapter around the external sans-I/O SCRAM state machine.

mod error;
mod nonce;
mod session;

#[cfg(test)]
mod fanout_bench_test;
#[cfg(test)]
mod session_test;

pub(in crate::authentication) use session::{ScramReceive, ScramSession};
