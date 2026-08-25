//! Data-only observation of semantic context ownership.

use calandria::RetainedBytes;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::reactor) struct OperationContextsSnapshot {
    reserved: usize,
    published: usize,
    retained_bytes: RetainedBytes,
    poisoned: bool,
}

impl OperationContextsSnapshot {
    pub(super) const fn new(
        reserved: usize,
        published: usize,
        retained_bytes: RetainedBytes,
        poisoned: bool,
    ) -> Self {
        Self {
            reserved,
            published,
            retained_bytes,
            poisoned,
        }
    }

    pub(in crate::reactor) const fn reserved(self) -> usize {
        self.reserved
    }

    pub(in crate::reactor) const fn published(self) -> usize {
        self.published
    }

    pub(in crate::reactor) const fn retained_bytes(self) -> RetainedBytes {
        self.retained_bytes
    }

    pub(in crate::reactor) const fn is_poisoned(self) -> bool {
        self.poisoned
    }
}
