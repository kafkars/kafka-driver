//! Single-owner allocation of bootstrap resolver effect identities.

use kafka_driver_core::EffectId;

#[derive(Debug)]
pub(super) struct BootstrapEffectIds {
    next: Option<u64>,
}

impl BootstrapEffectIds {
    pub(super) const fn new() -> Self {
        Self { next: Some(1) }
    }

    pub(super) fn reserve(&mut self) -> Option<EffectId> {
        let next = self.next?;
        self.next = next.checked_add(1);
        Some(EffectId::from_raw(next))
    }
}
