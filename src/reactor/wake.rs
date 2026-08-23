//! Cross-thread notification that preserves independent domain requests.

use std::{fmt, io};

/// Cloneable notification handle for waking a blocked reactor turn.
#[derive(Clone)]
pub struct WakeHandle {
    poller: calandria::WakeHandle,
}

impl WakeHandle {
    pub(in crate::reactor) fn new(poller: calandria::WakeHandle) -> Self {
        Self { poller }
    }

    pub(in crate::reactor) fn into_calandria(self) -> calandria::WakeHandle {
        self.poller
    }

    /// Requests reactor progress for this exact cross-thread transition.
    ///
    /// Mio may coalesce readiness that is still pending in the selector. The
    /// handle itself deliberately does not share an acknowledgement bit across
    /// independent reactor domains: one domain cannot suppress another's later
    /// request after the selector consumed an earlier event.
    pub fn wake(&self) -> io::Result<()> {
        let result = self.poller.wake();
        if result.is_ok() {
            self.poller.acknowledge();
        }
        result
    }
}

impl fmt::Debug for WakeHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("WakeHandle").finish_non_exhaustive()
    }
}
