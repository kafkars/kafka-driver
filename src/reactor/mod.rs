//! Single-owner host, bounded command mailbox, and cross-thread wake contract.

mod bootstrap;
mod broker;
mod clock;
mod command;
mod error;
mod host;
mod mailbox;
mod metadata;
mod plaintext;
mod poller;
mod resolver;
mod resource;
mod tcp;
mod timer;
#[cfg(feature = "tls-rustls")]
mod tls;
mod transport;
mod wake;

#[cfg(test)]
mod clock_test;
#[cfg(test)]
mod mailbox_test;

pub(crate) use command::Command;
pub use error::ReactorError;
pub use host::{Reactor, TurnOutcome};
pub(crate) use mailbox::{MailboxSender, TrySendError, mailbox};
pub(in crate::reactor) use poller::{PollEvent, PollInterest, PollWake, Poller, Readiness};
pub use wake::WakeHandle;
