//! Explicit bounds for one connection machine's pending work and diagnostics.

use std::num::NonZeroUsize;

const DEFAULT_MAX_IN_FLIGHT: NonZeroUsize = nonzero(256);
const DEFAULT_MAX_CAPABILITIES: NonZeroUsize = nonzero(128);
const DEFAULT_MAX_TRACE_RECORDS: usize = 128;

/// Resource bounds owned by one connection machine.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionLimits {
    in_flight: NonZeroUsize,
    capabilities: NonZeroUsize,
    trace_records: usize,
}

impl ConnectionLimits {
    /// Creates limits for pending calls and retained transition records.
    pub const fn new(max_in_flight: NonZeroUsize, max_trace_records: usize) -> Self {
        Self {
            in_flight: max_in_flight,
            capabilities: DEFAULT_MAX_CAPABILITIES,
            trace_records: max_trace_records,
        }
    }

    /// Replaces the maximum negotiated APIs retained for one epoch.
    pub const fn with_max_capabilities(mut self, max_capabilities: NonZeroUsize) -> Self {
        self.capabilities = max_capabilities;
        self
    }

    /// Returns the maximum calls awaiting a response on this connection.
    pub const fn max_in_flight(self) -> NonZeroUsize {
        self.in_flight
    }

    /// Returns the maximum negotiated APIs retained for one epoch.
    pub const fn max_capabilities(self) -> NonZeroUsize {
        self.capabilities
    }

    /// Returns the maximum sanitized transition records retained in memory.
    pub const fn max_trace_records(self) -> usize {
        self.trace_records
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
