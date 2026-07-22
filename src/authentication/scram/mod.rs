//! SCRAM transcript ownership split into parsing, proof, nonce, and state seams.

mod algorithm;
mod client_first;
mod limits;
mod message;
mod nonce;
mod proof;
mod session;

#[cfg(test)]
mod fanout_bench_test;
#[cfg(test)]
mod message_test;
#[cfg(test)]
mod session_test;

pub(super) use session::ScramSession;
