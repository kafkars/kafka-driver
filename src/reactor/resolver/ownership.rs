//! Bounded mapping from global DNS effect identities to shard-local policy owners.

use std::{collections::BTreeMap, fmt, num::NonZeroUsize};

use kafka_driver_core::EffectId;

use crate::reactor::{BrokerLane, direct_plaintext::endpoint_refresh::DirectRefreshOwner};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum ResolutionOwner {
    Bootstrap,
    Broker(BrokerLane),
    Direct(DirectRefreshOwner),
}

pub(in crate::reactor) struct ResolverOwnership {
    owners: BTreeMap<EffectId, ResolutionOwner>,
    capacity: NonZeroUsize,
}

impl ResolverOwnership {
    pub(in crate::reactor) const fn new(capacity: NonZeroUsize) -> Self {
        Self {
            owners: BTreeMap::new(),
            capacity,
        }
    }

    pub(in crate::reactor) fn register(
        &mut self,
        effect_id: EffectId,
        owner: ResolutionOwner,
    ) -> Result<(), ResolverOwnershipError> {
        if self.owners.contains_key(&effect_id) {
            return Err(ResolverOwnershipError::Duplicate);
        }
        if self.owners.len() == self.capacity.get() {
            return Err(ResolverOwnershipError::CapacityReached {
                limit: self.capacity.get(),
            });
        }
        self.owners.insert(effect_id, owner);
        Ok(())
    }

    pub(in crate::reactor) fn remove(&mut self, effect_id: EffectId) -> Option<ResolutionOwner> {
        self.owners.remove(&effect_id)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum ResolverOwnershipError {
    CapacityReached { limit: usize },
    Duplicate,
}

impl fmt::Display for ResolverOwnershipError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapacityReached { limit } => {
                write!(formatter, "resolver ownership capacity {limit} reached")
            }
            Self::Duplicate => formatter.write_str("resolver effect identity is already owned"),
        }
    }
}

impl std::error::Error for ResolverOwnershipError {}
