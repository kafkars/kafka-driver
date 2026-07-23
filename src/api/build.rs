//! Public construction failures before one driver owns a usable target.

use std::{fmt, io};

/// Why one driver target and its purpose-built I/O shard could not be built.
#[non_exhaustive]
#[derive(Debug)]
pub enum DriverBuildError {
    /// No direct broker or bootstrap set was configured.
    MissingTarget,
    /// Every process-local driver authority identity has been allocated.
    IdentityExhausted,
    /// The purpose-built reactor could not acquire its operating-system resources.
    Reactor(io::Error),
}

impl DriverBuildError {
    pub(crate) const fn new(source: io::Error) -> Self {
        Self::Reactor(source)
    }
}

impl fmt::Display for DriverBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingTarget => {
                formatter.write_str("a direct broker or bootstrap target is required")
            }
            Self::IdentityExhausted => {
                formatter.write_str("the driver authority identity space is exhausted")
            }
            Self::Reactor(_) => formatter.write_str("failed to create the driver I/O shard"),
        }
    }
}

impl std::error::Error for DriverBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Reactor(source) => Some(source),
            Self::MissingTarget | Self::IdentityExhausted => None,
        }
    }
}
