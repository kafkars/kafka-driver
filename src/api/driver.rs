//! Public construction, admission, and shutdown handle for one driver reactor.

use std::{fmt, io};

use crate::{
    completion::completion_pair,
    config::DriverLimits,
    reactor::{Command, MailboxSender, Reactor, TrySendError},
};

use super::{Call, DriverBuildError};

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
    pub fn build_reactor(self) -> Result<(Driver, Reactor), DriverBuildError> {
        let (commands, reactor) = Reactor::new(self.limits).map_err(DriverBuildError::new)?;
        Ok((Driver { commands }, reactor))
    }
}

/// Why a command could not enter the bounded reactor mailbox.
#[derive(Debug)]
pub enum SubmitError {
    /// The mailbox has reached its configured command capacity.
    Full,
    /// The reactor has closed command admission permanently.
    Closed,
    /// The command remained unadmitted because the OS poller could not be woken.
    Wake(io::Error),
}

impl fmt::Display for SubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => formatter.write_str("the driver command mailbox is full"),
            Self::Closed => formatter.write_str("the driver command mailbox is closed"),
            Self::Wake(_) => formatter.write_str("the driver I/O shard could not be woken"),
        }
    }
}

impl std::error::Error for SubmitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Wake(source) => Some(source),
            Self::Full | Self::Closed => None,
        }
    }
}

impl From<TrySendError<Command>> for SubmitError {
    fn from(error: TrySendError<Command>) -> Self {
        match error {
            TrySendError::Full(_) => Self::Full,
            TrySendError::Closed(_) => Self::Closed,
            TrySendError::Wake { command, source } => {
                drop(command);
                Self::Wake(source)
            }
        }
    }
}
