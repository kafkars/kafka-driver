//! Public bounded-admission failure vocabulary for driver work.

use std::{fmt, io};

use crate::{
    completion::ShutdownSubscribeError,
    reactor::{Command, TrySendError},
};

/// Why work could not enter a bounded driver admission point.
#[derive(Debug)]
pub enum SubmitError {
    /// A command lane or the shutdown barrier has reached its configured capacity.
    Full,
    /// The reactor has closed command admission permanently.
    Closed,
    /// The command remained unadmitted because the OS poller could not be woken.
    Wake(io::Error),
    /// Every public call identity has been allocated for this driver instance.
    IdentityExhausted,
    /// An invalidation token was issued by another driver instance.
    ForeignDriver,
}

impl fmt::Display for SubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => formatter.write_str("the driver admission capacity is full"),
            Self::Closed => formatter.write_str("the driver is closed to admission"),
            Self::Wake(_) => formatter.write_str("the driver I/O shard could not be woken"),
            Self::IdentityExhausted => {
                formatter.write_str("the driver call identity space is exhausted")
            }
            Self::ForeignDriver => {
                formatter.write_str("the route failure token belongs to another driver")
            }
        }
    }
}

impl std::error::Error for SubmitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Wake(source) => Some(source),
            Self::Full | Self::Closed | Self::IdentityExhausted | Self::ForeignDriver => None,
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

impl From<ShutdownSubscribeError<TrySendError<Command>>> for SubmitError {
    fn from(error: ShutdownSubscribeError<TrySendError<Command>>) -> Self {
        match error {
            ShutdownSubscribeError::Full => Self::Full,
            ShutdownSubscribeError::Closed => Self::Closed,
            ShutdownSubscribeError::Request(error) => error.into(),
        }
    }
}
