//! Sanitized debug surface for the compatibility connection machine.

use std::fmt;

use super::ConnectionMachine;

impl fmt::Debug for ConnectionMachine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionMachine")
            .field("state", &self.state())
            .field("pending_count", &self.pending_count())
            .finish_non_exhaustive()
    }
}
