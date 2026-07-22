//! Public failure from driving the operating-system poll selector.

use std::{fmt, io};

/// Why one reactor turn could not observe external readiness.
#[derive(Debug)]
pub struct ReactorError {
    source: io::Error,
}

impl ReactorError {
    pub(super) const fn poll(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for ReactorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the driver I/O selector failed")
    }
}

impl std::error::Error for ReactorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
