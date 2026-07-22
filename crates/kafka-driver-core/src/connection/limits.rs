//! Explicit bounds for one connection machine's pending work and diagnostics.

use std::num::NonZeroUsize;

const DEFAULT_MAX_IN_FLIGHT: NonZeroUsize = nonzero(256);
const DEFAULT_MAX_TRACE_RECORDS: usize = 128;

/// Resource bounds owned by one connection machine.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionLimits {
    max_in_flight: NonZeroUsize,
    max_trace_records: usize,
}

impl ConnectionLimits {
    /// Creates limits for pending calls and retained transition records.
    pub const fn new(max_in_flight: NonZeroUsize, max_trace_records: usize) -> Self {
        Self {
            max_in_flight,
            max_trace_records,
        }
    }

    /// Returns the maximum calls awaiting a response on this connection.
    pub const fn max_in_flight(self) -> NonZeroUsize {
        self.max_in_flight
    }

    /// Returns the maximum sanitized transition records retained in memory.
    pub const fn max_trace_records(self) -> usize {
        self.max_trace_records
    }
}

impl Default for ConnectionLimits {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_IN_FLIGHT, DEFAULT_MAX_TRACE_RECORDS)
    }
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("connection defaults must be nonzero");
    };
    value
}
