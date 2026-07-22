//! Lock-free allocation of public call identities before mailbox admission.

use std::sync::atomic::{AtomicU64, Ordering};

use kafka_driver_core::CallId;

#[derive(Debug)]
pub(super) struct CallIds {
    next: AtomicU64,
}

impl CallIds {
    pub(super) const fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    pub(super) fn allocate(&self) -> Option<CallId> {
        self.next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
            .ok()
            .map(CallId::from_raw)
    }
}
