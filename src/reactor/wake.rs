//! Coalesced cross-thread notification for selector and mailbox progress.

use std::{
    fmt, io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use super::PollWake;

/// Cloneable notification handle for waking a blocked reactor turn.
#[derive(Clone)]
pub struct WakeHandle {
    shared: Arc<WakeState>,
}

impl WakeHandle {
    pub(in crate::reactor) fn new(poller: PollWake) -> Self {
        Self {
            shared: Arc::new(WakeState {
                requested: AtomicBool::new(false),
                poller,
            }),
        }
    }

    /// Requests reactor progress, coalescing repeated requests until acknowledged.
    pub fn wake(&self) -> io::Result<()> {
        if self
            .shared
            .requested
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            && let Err(source) = self.shared.poller.wake()
        {
            self.shared.requested.store(false, Ordering::Release);
            return Err(source);
        }
        Ok(())
    }

    pub(crate) fn acknowledge(&self) {
        self.shared.requested.store(false, Ordering::Release);
    }

    pub(crate) fn is_requested(&self) -> bool {
        self.shared.requested.load(Ordering::Acquire)
    }
}

impl fmt::Debug for WakeHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WakeHandle")
            .field("requested", &self.is_requested())
            .finish()
    }
}

struct WakeState {
    requested: AtomicBool,
    poller: PollWake,
}
