//! Single-owner allocation of logical metadata refresh identities.

use kafka_driver_core::OperationId;

#[derive(Debug)]
pub(super) struct MetadataOperationIds {
    next: Option<u64>,
}

impl MetadataOperationIds {
    pub(super) const fn new() -> Self {
        Self { next: Some(1) }
    }

    #[cfg(test)]
    pub(super) const fn starting_at(next: u64) -> Self {
        Self { next: Some(next) }
    }

    pub(super) fn reserve(&mut self) -> Option<OperationId> {
        let current = self.next?;
        self.next = current.checked_add(1);
        Some(OperationId::from_raw(current))
    }
}
