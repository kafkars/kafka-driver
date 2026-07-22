//! Single-owner host, bounded command mailbox, and cross-thread wake contract.

#[allow(
    dead_code,
    reason = "M4 broker identities are consumed by the broker owner in the next slice"
)]
mod broker;
#[allow(
    dead_code,
    reason = "M4 clock drives connection deadlines in the broker-owner slice"
)]
mod clock;
mod command;
mod error;
mod host;
mod mailbox;
#[allow(
    dead_code,
    reason = "M4 plaintext progress is wired into resource effects in the next slice"
)]
mod plaintext;
#[allow(
    dead_code,
    reason = "M4 poll adapter is consumed by the host in the next integration slice"
)]
mod poller;
#[allow(
    dead_code,
    reason = "M4 locks generational resource identity before the poll adapter consumes it"
)]
mod resource;
#[allow(
    dead_code,
    reason = "M4 locks bounded deadline ordering before the poll host consumes it"
)]
mod timer;
mod wake;

#[cfg(test)]
mod clock_test;
#[cfg(test)]
mod mailbox_test;

pub(crate) use command::Command;
pub use error::ReactorError;
pub use host::{Reactor, TurnOutcome};
pub(crate) use mailbox::{MailboxSender, TrySendError, mailbox};
pub(in crate::reactor) use poller::{PollEvent, PollInterest, PollWake, Poller};
pub use wake::WakeHandle;
