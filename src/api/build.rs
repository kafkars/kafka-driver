//! Public construction failure for operating-system reactor resources.

use std::{fmt, io};

/// Why the driver could not create its purpose-built I/O shard.
#[derive(Debug)]
pub struct DriverBuildError {
    source: io::Error,
}

impl DriverBuildError {
    pub(crate) const fn new(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DriverBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("failed to create the driver I/O shard")
    }
}

impl std::error::Error for DriverBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
