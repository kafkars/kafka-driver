//! States containing only data valid before or during one bootstrap retry wait.

use crate::{Moment, RetryOrdinal};

/// Current retry ordinal or one owned driver-relative retry deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapRetryState {
    /// A DNS pass may run, and its next exhaustion uses this ordinal.
    Ready {
        /// One-based ordinal for the next bounded delay.
        retry: RetryOrdinal,
    },
    /// A failed pass is waiting for its exact retry deadline.
    Waiting {
        /// Ordinal that produced this delay.
        retry: RetryOrdinal,
        /// Earliest instant at which another pass may start.
        at: Moment,
    },
}
