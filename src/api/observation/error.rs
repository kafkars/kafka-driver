//! Public terminal failure for an admitted observation command.

use std::fmt;

/// Why an admitted snapshot command could not return a running-owner view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SnapshotError {
    /// Graceful shutdown won priority before this snapshot was interpreted.
    Draining,
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the driver began draining before snapshot interpretation")
    }
}

impl std::error::Error for SnapshotError {}
