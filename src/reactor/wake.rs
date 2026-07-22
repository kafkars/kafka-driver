//! Coalesced cross-thread notification for mailbox and embedded-host progress.

use std::{
    fmt,
    sync::{Arc, Condvar, Mutex, MutexGuard},
    time::Duration,
};

/// Cloneable notification handle for waking a blocked reactor turn.
#[derive(Clone)]
pub struct WakeHandle {
    shared: Arc<WakeState>,
}

impl WakeHandle {
    pub(crate) fn new() -> Self {
        Self {
            shared: Arc::new(WakeState {
                requested: Mutex::new(false),
                ready: Condvar::new(),
            }),
        }
    }

    /// Requests reactor progress, coalescing repeated requests until acknowledged.
    pub fn wake(&self) {
        let mut requested = self.lock();
        if !*requested {
            *requested = true;
            self.shared.ready.notify_one();
        }
    }

    pub(crate) fn wait(&self, timeout: Duration) -> bool {
        let requested = self.lock();
        let (requested, _timeout) = self
            .shared
            .ready
            .wait_timeout_while(requested, timeout, |requested| !*requested)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *requested
    }

    pub(crate) fn acknowledge(&self) {
        *self.lock() = false;
    }

    pub(crate) fn is_requested(&self) -> bool {
        *self.lock()
    }

    fn lock(&self) -> MutexGuard<'_, bool> {
        self.shared
            .requested
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
    requested: Mutex<bool>,
    ready: Condvar,
}
