//! Bounded off-shard SCRAM proof derivation with exact connection fencing.

mod error;
mod handle;
mod request;
mod worker;

#[cfg(test)]
mod queue_test;
#[cfg(test)]
mod worker_test;

#[cfg(test)]
pub(in crate::reactor) use queue_test::request as proof_request;

pub(in crate::reactor) use error::{ScramProofSubmitError, ScramProofWorkerError};
pub(in crate::reactor) use handle::{ScramProofSender, ScramProofWorker};
pub(in crate::reactor) use request::{ScramProofOutcome, ScramProofRequest};
