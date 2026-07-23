//! Atomic current and cumulative mailbox-pressure snapshot construction.

use std::sync::atomic::Ordering;

use super::{MailboxReceiver, ownership::MailboxLane};

impl<T> MailboxReceiver<T> {
    pub(crate) fn snapshot(&self) -> crate::MailboxSnapshot {
        let state = self.shared.lock();
        crate::MailboxSnapshot::new(
            self.shared.capacity,
            self.shared.byte_capacity,
            [
                state.queued(MailboxLane::Work),
                state.queued_bytes(MailboxLane::Work),
                state.queued(MailboxLane::Control),
                state.queued_bytes(MailboxLane::Control),
            ],
            [
                self.shared.work_full.load(Ordering::Relaxed),
                self.shared.work_byte_full.load(Ordering::Relaxed),
                self.shared.control_full.load(Ordering::Relaxed),
                self.shared.control_byte_full.load(Ordering::Relaxed),
            ],
            [
                self.shared.closed_rejections.load(Ordering::Relaxed),
                self.shared.wake_failures.load(Ordering::Relaxed),
            ],
        )
    }
}
