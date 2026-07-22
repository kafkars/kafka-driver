//! Shard-owned allocation of globally unambiguous resolver effect identities.

use kafka_driver_core::EffectId;

#[derive(Debug)]
pub(in crate::reactor) struct ResolverEffectIds {
    next: Option<u64>,
}

impl ResolverEffectIds {
    pub(in crate::reactor) const fn new() -> Self {
        Self { next: Some(1) }
    }

    pub(in crate::reactor) fn reserve(&mut self) -> Option<EffectId> {
        let next = self.next?;
        self.next = next.checked_add(1);
        Some(EffectId::from_raw(next))
    }
}
