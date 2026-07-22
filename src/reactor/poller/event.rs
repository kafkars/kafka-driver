//! Poll outcomes separated into administrative wake and resource readiness.

use crate::reactor::resource::ResourceToken;

use super::Readiness;

/// One external readiness observation returned by the reactor poller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum PollEvent {
    /// Administrative work requested progress from another thread.
    Wake,
    /// One generation-checked resource may make nonblocking progress.
    Resource {
        /// Generational token identifying the registered resource slot.
        token: ResourceToken,
        /// Read, write, closure, and error readiness reported by Mio.
        readiness: Readiness,
    },
}
