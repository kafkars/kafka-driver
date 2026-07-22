//! Bounded FIFO ownership and partial progress for complete encoded request frames.

mod error;
mod limits;
mod queue;
mod state;

#[cfg(test)]
mod queue_test;

pub use error::{
    WriteAdmissionError, WriteAdmissionFailure, WriteIdentityKind, WriteProgressError,
};
pub use limits::WriteQueueLimits;
pub use queue::WriteQueue;
pub use state::{DiscardedWrites, WriteAccepted, WriteProgress, WriteSlice};
