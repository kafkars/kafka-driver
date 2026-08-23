//! Broker-slot validation for Calandria resource owner identities.

use std::num::NonZeroUsize;

use calandria::ResourceOwnerId;

/// One broker slot's disjoint share of the poll-token identity space.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct ResourceNamespace {
    owner: ResourceOwnerId,
}

impl ResourceNamespace {
    pub(in crate::reactor) fn new(owner_slot: usize, owner_capacity: NonZeroUsize) -> Option<Self> {
        if owner_slot >= owner_capacity.get() {
            return None;
        }
        let owner = u64::try_from(owner_slot).ok().map(ResourceOwnerId::new)?;
        Some(Self { owner })
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn single() -> Self {
        Self {
            owner: ResourceOwnerId::new(0),
        }
    }

    pub(super) const fn owner(self) -> ResourceOwnerId {
        self.owner
    }
}
