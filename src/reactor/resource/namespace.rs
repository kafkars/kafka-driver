//! Bounded broker-slot ownership embedded into generational poll tokens.

use std::num::NonZeroUsize;

/// One broker slot's disjoint share of the poll-token identity space.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct ResourceNamespace {
    owner_slot: usize,
    owner_capacity: NonZeroUsize,
}

impl ResourceNamespace {
    pub(in crate::reactor) const fn new(
        owner_slot: usize,
        owner_capacity: NonZeroUsize,
    ) -> Option<Self> {
        if owner_slot >= owner_capacity.get() {
            return None;
        }
        Some(Self {
            owner_slot,
            owner_capacity,
        })
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn single() -> Self {
        let Some(namespace) = Self::new(0, NonZeroUsize::MIN) else {
            panic!("single resource namespace must be valid");
        };
        namespace
    }

    pub(in crate::reactor) const fn owner_slot(self) -> usize {
        self.owner_slot
    }

    pub(in crate::reactor) const fn owner_capacity(self) -> NonZeroUsize {
        self.owner_capacity
    }
}
