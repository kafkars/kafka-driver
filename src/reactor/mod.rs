//! Single-owner host, bounded command mailbox, and cross-thread wake contract.

mod command;
mod host;
mod mailbox;
#[allow(
    dead_code,
    reason = "M4 locks bounded deadline ordering before the poll host consumes it"
)]
mod timer;
mod wake;

#[cfg(test)]
mod mailbox_test;

pub(crate) use command::Command;
pub use host::{Reactor, TurnOutcome};
pub(crate) use mailbox::{MailboxSender, TrySendError, mailbox};
pub use wake::WakeHandle;
