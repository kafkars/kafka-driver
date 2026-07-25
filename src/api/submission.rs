//! Public bounded-admission failure vocabulary for driver work.

use std::{fmt, io};

use kafka_wire_core::{ApiKey, ApiVersion};

use crate::{
    completion::ShutdownSubscribeError,
    reactor::{Command, TrySendError},
};

/// Why work could not enter a bounded driver admission point.
///
/// When returned by request admission, every variant leaves that request
/// definitely unsent because no command entered reactor ownership.
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
    /// A request supplied a minimum API version above its maximum.
    VersionBoundsInvalid {
        /// Generated Kafka API key requested by the call.
        api_key: ApiKey,
        /// Least version the caller permits for this request.
        minimum: ApiVersion,
        /// Greatest version the caller permits for this request.
        maximum: ApiVersion,
    },
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
            Self::VersionBoundsInvalid {
                api_key,
                minimum,
                maximum,
            } => write!(
                formatter,
                "Kafka API {api_key} request minimum {minimum} exceeds request maximum {maximum}"
            ),
        }
    }
}

impl std::error::Error for SubmitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Wake(source) => Some(source),
            Self::Full
            | Self::Closed
            | Self::IdentityExhausted
            | Self::ForeignDriver
            | Self::VersionBoundsInvalid { .. } => None,
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
