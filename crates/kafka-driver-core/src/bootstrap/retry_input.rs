//! Driver-relative observations accepted by bootstrap retry policy.

use crate::{JitterSample, Moment};

/// One bootstrap retry observation supplied by the reactor interpreter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapRetryInput {
    /// Every configured endpoint failed in the current pass.
    Exhausted {
        /// Driver-relative instant at which the pass ended.
        now: Moment,
        /// Reactor-owned entropy reduced to deterministic input data.
        jitter: JitterSample,
    },
    /// The host reached or approached the owned retry deadline.
    Elapsed {
        /// Driver-relative instant observed by the host.
        now: Moment,
    },
    /// A bootstrap pass produced a usable address set.
    Succeeded,
}
