//! Single-owner allocation of coordinator discovery operation identities.

use kafka_driver_core::OperationId;

pub(super) struct CoordinatorOperationIds {
    next: Option<u64>,
}

impl CoordinatorOperationIds {
    pub(super) const fn new() -> Self {
        Self { next: Some(1) }
    }

    pub(super) fn reserve(&mut self) -> Option<OperationId> {
        let raw = self.next?;
        self.next = raw.checked_add(1);
        Some(OperationId::from_raw(raw))
    }
}
