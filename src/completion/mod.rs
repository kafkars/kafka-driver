//! Runtime-neutral handoff between the reactor and one public call.

mod outcome;
mod receiver;
mod sender;
mod state;

#[cfg(test)]
mod completion_test;

pub use outcome::{CancellationRequest, CompletionError};
pub(crate) use receiver::CompletionReceiver;
pub(crate) use sender::CompletionSender;
pub(crate) use state::completion_pair;
