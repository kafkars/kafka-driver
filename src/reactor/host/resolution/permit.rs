//! Pre-transition ownership of one bounded DNS effect and fallback request slot.

use kafka_driver_core::EffectId;

use crate::reactor::{
    broker_set::BrokerLane,
    resolver::{ResolutionOwner, ResolverOwnershipError},
};

use super::{NameResolution, NameResolutionError};

#[derive(Debug)]
#[must_use = "a DNS reservation must be submitted or cancelled"]
pub(in crate::reactor::host) struct ResolutionPermit {
    effect_id: EffectId,
    owner: ResolutionOwner,
}

impl ResolutionPermit {
    pub(super) const fn new(effect_id: EffectId, owner: ResolutionOwner) -> Self {
        Self { effect_id, owner }
    }

    pub(in crate::reactor::host) const fn effect_id(&self) -> EffectId {
        self.effect_id
    }

    pub(in crate::reactor::host) const fn owner(&self) -> ResolutionOwner {
        self.owner
    }
}

impl NameResolution {
    pub(in crate::reactor::host) fn try_reserve_broker(
        &mut self,
        lane: BrokerLane,
    ) -> Result<Option<ResolutionPermit>, NameResolutionError> {
        self.try_reserve(ResolutionOwner::Broker(lane))
    }

    pub(in crate::reactor::host) const fn capacity(&self) -> usize {
        self.pending.capacity()
    }

    pub(super) fn try_reserve(
        &mut self,
        owner: ResolutionOwner,
    ) -> Result<Option<ResolutionPermit>, NameResolutionError> {
        if !self.pending.try_reserve() {
            return Ok(None);
        }
        let Some(effect_id) = self.effect_ids.reserve() else {
            self.pending.release_reservation();
            return Err(NameResolutionError::IdentityExhausted);
        };
        match self.ownership.register(effect_id, owner) {
            Ok(()) => Ok(Some(ResolutionPermit::new(effect_id, owner))),
            Err(ResolverOwnershipError::CapacityReached { .. }) => {
                self.pending.release_reservation();
                Ok(None)
            }
            Err(error) => {
                self.pending.release_reservation();
                Err(error.into())
            }
        }
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "cancellation consumes the linear reservation token"
    )]
    pub(in crate::reactor::host) fn cancel(&mut self, permit: ResolutionPermit) {
        self.pending.release_reservation();
        debug_assert_eq!(
            self.ownership.remove(permit.effect_id()),
            Some(permit.owner())
        );
    }
}
