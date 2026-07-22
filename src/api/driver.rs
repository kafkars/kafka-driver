//! Public construction, admission, and shutdown handle for one driver reactor.

use std::fmt;

use crate::{
    completion::completion_pair,
    config::DriverLimits,
    reactor::{Command, MailboxSender, Reactor, TrySendError, mailbox},
};

use super::Call;

/// Cloneable command-admission handle for one driver reactor.
#[derive(Clone, Debug)]
pub struct Driver {
    commands: MailboxSender<Command>,
}

impl Driver {
    /// Starts configuration of a driver and its reactor host.
    pub fn builder() -> DriverBuilder {
        DriverBuilder::default()
    }

    /// Requests terminal shutdown through the bounded command mailbox.
    pub fn shutdown(&self) -> Result<Call<()>, SubmitError> {
        let (completion, sender) = completion_pair();
        let call = Call::new(completion);
        let command = Command::Shutdown { completion: sender };
        self.commands.try_send(command).map_err(SubmitError::from)?;
        Ok(call)
    }
}

/// Builder for one command handle and embedded reactor pair.
#[derive(Clone, Copy, Debug, Default)]
pub struct DriverBuilder {
    limits: DriverLimits,
}

impl DriverBuilder {
    /// Replaces the default admission and fairness limits.
    #[must_use]
    pub const fn limits(mut self, limits: DriverLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Builds a driver handle and an embedded, caller-driven reactor.
    pub fn build_reactor(self) -> (Driver, Reactor) {
        let (sender, receiver) = mailbox(self.limits.mailbox_capacity());
        (
            Driver { commands: sender },
            Reactor::new(receiver, self.limits.command_budget()),
        )
    }
}

/// Why a command could not enter the bounded reactor mailbox.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmitError {
    /// The mailbox has reached its configured command capacity.
    Full,
    /// The reactor has closed command admission permanently.
    Closed,
}

impl fmt::Display for SubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => formatter.write_str("the driver command mailbox is full"),
            Self::Closed => formatter.write_str("the driver command mailbox is closed"),
        }
    }
}

impl std::error::Error for SubmitError {}

impl From<TrySendError<Command>> for SubmitError {
    fn from(error: TrySendError<Command>) -> Self {
        match error {
            TrySendError::Full(_) => Self::Full,
            TrySendError::Closed(_) => Self::Closed,
        }
    }
}
