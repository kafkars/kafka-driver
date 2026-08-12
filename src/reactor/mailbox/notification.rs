//! Mailbox-local wake coalescing acknowledged only by mailbox drain.

use std::{
    io,
    sync::atomic::{AtomicBool, Ordering},
};

use crate::reactor::WakeHandle;

pub(super) struct MailboxNotification {
    requested: AtomicBool,
    handle: WakeHandle,
}

impl MailboxNotification {
    pub(super) const fn new(handle: WakeHandle) -> Self {
        Self {
            requested: AtomicBool::new(false),
            handle,
        }
    }

    pub(super) fn request(&self) -> io::Result<()> {
        if self
            .requested
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            && let Err(source) = self.handle.wake()
        {
            self.requested.store(false, Ordering::Release);
            return Err(source);
        }
        Ok(())
    }

    pub(super) fn acknowledge(&self) {
        self.requested.store(false, Ordering::Release);
    }

    pub(super) fn handle(&self) -> WakeHandle {
        self.handle.clone()
    }

    #[cfg(test)]
    pub(super) fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}
