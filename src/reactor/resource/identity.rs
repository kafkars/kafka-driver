//! Typed connection identity and generation-bearing poll token.

use kafka_driver_core::{ConnectionEpoch, TransportId};

use super::ResourceNamespace;

/// Identities a readiness event must echo before it can affect a connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct ResourceIdentity {
    transport_id: TransportId,
    epoch: ConnectionEpoch,
}

impl ResourceIdentity {
    pub(in crate::reactor) const fn new(transport_id: TransportId, epoch: ConnectionEpoch) -> Self {
        Self {
            transport_id,
            epoch,
        }
    }

    pub(in crate::reactor) const fn transport_id(self) -> TransportId {
        self.transport_id
    }

    pub(in crate::reactor) const fn epoch(self) -> ConnectionEpoch {
        self.epoch
    }
}

/// Opaque poll token encoding one bounded slot and its reuse generation.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::reactor) struct ResourceToken(usize);

impl ResourceToken {
    pub(in crate::reactor) const fn get(self) -> usize {
        self.0
    }

    #[cfg(test)]
    pub(super) fn encode(capacity: usize, slot: usize, generation: usize) -> Option<Self> {
        Self::encode_in(capacity, ResourceNamespace::single(), slot, generation)
    }

    #[cfg(test)]
    pub(super) const fn decode(self, capacity: usize) -> Option<(usize, usize)> {
        self.decode_in(capacity, ResourceNamespace::single())
    }

    pub(super) fn encode_in(
        resource_capacity: usize,
        namespace: ResourceNamespace,
        resource_slot: usize,
        generation: usize,
    ) -> Option<Self> {
        if resource_slot >= resource_capacity {
            return None;
        }
        let owner_stride = resource_capacity.checked_mul(namespace.owner_capacity().get())?;
        generation
            .checked_mul(owner_stride)?
            .checked_add(namespace.owner_slot().checked_mul(resource_capacity)?)?
            .checked_add(resource_slot)?
            .checked_add(1)
            .map(Self)
    }

    pub(super) const fn decode_in(
        self,
        resource_capacity: usize,
        namespace: ResourceNamespace,
    ) -> Option<(usize, usize)> {
        let Some(encoded) = self.0.checked_sub(1) else {
            return None;
        };
        if resource_capacity == 0 {
            return None;
        }
        let resource_slot = encoded % resource_capacity;
        let owner_and_generation = encoded / resource_capacity;
        let owner_capacity = namespace.owner_capacity().get();
        let Some(owner_slot) = self.owner(resource_capacity, owner_capacity) else {
            return None;
        };
        if owner_slot != namespace.owner_slot() {
            return None;
        }
        Some((resource_slot, owner_and_generation / owner_capacity))
    }

    pub(in crate::reactor) const fn owner(
        self,
        resource_capacity: usize,
        owner_capacity: usize,
    ) -> Option<usize> {
        let Some(encoded) = self.0.checked_sub(1) else {
            return None;
        };
        if resource_capacity == 0 || owner_capacity == 0 {
            return None;
        }
        Some((encoded / resource_capacity) % owner_capacity)
    }

    pub(in crate::reactor) const fn from_poll(raw: usize) -> Option<Self> {
        if raw == 0 { None } else { Some(Self(raw)) }
    }

    #[cfg(test)]
    pub(super) const fn from_raw(raw: usize) -> Self {
        Self(raw)
    }
}
