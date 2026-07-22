//! Typed connection identity and generation-bearing poll token.

use kafka_driver_core::{ConnectionEpoch, TransportId};

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

    pub(super) fn encode(capacity: usize, slot: usize, generation: usize) -> Option<Self> {
        generation
            .checked_mul(capacity)?
            .checked_add(slot)?
            .checked_add(1)
            .map(Self)
    }

    pub(super) const fn decode(self, capacity: usize) -> Option<(usize, usize)> {
        let Some(encoded) = self.0.checked_sub(1) else {
            return None;
        };
        Some((encoded % capacity, encoded / capacity))
    }

    #[cfg(test)]
    pub(super) const fn from_raw(raw: usize) -> Self {
        Self(raw)
    }
}
