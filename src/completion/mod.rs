//! Runtime-neutral handoff between the reactor and one public call.

mod outcome;
mod receiver;
mod sender;
mod shutdown;
mod state;

#[cfg(test)]
mod completion_test;
#[cfg(test)]
mod shutdown_test;

pub use outcome::CompletionError;
pub(crate) use receiver::CompletionReceiver;
pub(crate) use sender::CompletionSender;
pub(crate) use shutdown::{
    ShutdownCompleter, ShutdownRequester, ShutdownSubscribeError, shutdown_barrier,
};
pub(crate) use state::completion_pair;
