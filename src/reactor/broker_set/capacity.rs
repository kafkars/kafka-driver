//! Coherent broker, lane, resource-owner, and poll-registration bounds.

use std::num::NonZeroUsize;

use crate::{MetadataLimits, TrafficClass, reactor::broker::BrokerLimits};

use super::{BrokerSet, BrokerSetError};

#[derive(Clone, Copy, Debug)]
pub(super) struct BrokerSetCapacity {
    brokers: NonZeroUsize,
    lanes: NonZeroUsize,
    owners: NonZeroUsize,
    registrations: NonZeroUsize,
}

impl BrokerSetCapacity {
    pub(super) fn new(
        broker_limits: BrokerLimits,
        metadata_limits: MetadataLimits,
    ) -> Result<Self, BrokerSetError> {
        let brokers = metadata_limits.broker_directory().max_brokers();
        let lanes = brokers
            .get()
            .checked_mul(TrafficClass::COUNT)
            .and_then(NonZeroUsize::new)
            .ok_or(BrokerSetError::OwnerCapacityOverflow)?;
        let owners = lanes
            .get()
            .checked_add(1)
            .and_then(NonZeroUsize::new)
            .ok_or(BrokerSetError::OwnerCapacityOverflow)?;
        let registrations = broker_limits
            .resource_capacity()
            .get()
            .checked_mul(owners.get())
            .and_then(NonZeroUsize::new)
            .ok_or(BrokerSetError::OwnerCapacityOverflow)?;
        Ok(Self {
            brokers,
            lanes,
            owners,
            registrations,
        })
    }

    pub(super) const fn brokers(self) -> NonZeroUsize {
        self.brokers
    }

    pub(super) const fn lanes(self) -> NonZeroUsize {
        self.lanes
    }

    pub(super) const fn owners(self) -> NonZeroUsize {
        self.owners
    }
}

impl BrokerSet {
    pub(in crate::reactor) fn poll_registration_capacity(
        broker_limits: BrokerLimits,
        metadata_limits: MetadataLimits,
    ) -> Result<NonZeroUsize, BrokerSetError> {
        BrokerSetCapacity::new(broker_limits, metadata_limits).map(BrokerSetCapacity::registrations)
    }
}

impl BrokerSetCapacity {
    const fn registrations(self) -> NonZeroUsize {
        self.registrations
    }
}
