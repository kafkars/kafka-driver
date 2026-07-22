//! Bounded waiting and retry work emitted by bootstrap retry policy.

use crate::Moment;

/// One external action authorized by a bootstrap retry transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapRetryEffect {
    /// Keep the host wake deadline at this driver-relative instant.
    WaitUntil {
        /// Earliest instant at which a new pass may begin.
        at: Moment,
    },
    /// Begin one fresh identity-fenced pass through configured endpoints.
    Restart,
}
