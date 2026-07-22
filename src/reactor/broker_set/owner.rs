//! Bounded seed-child delegation behind one future-proof namespace owner.

use std::num::NonZeroUsize;

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryLimits, BrokerState, ConnectionState, MetadataGeneration,
    Moment,
};

use crate::{
    config::BrokerConfig,
    reactor::{
        PollEvent, Poller,
        broker::{BrokerLimits, DeadlineProgress, SingleBroker},
        resource::ResourceNamespace,
    },
    request::ErasedRequest,
};

use super::BrokerSetError;

/// Shard-local owner of a seed connection and disjoint broker token namespaces.
pub(in crate::reactor) struct BrokerSet {
    seed: Option<SingleBroker>,
    directory: Option<BrokerDirectory>,
    broker_limits: BrokerLimits,
    owner_capacity: NonZeroUsize,
}

impl BrokerSet {
    pub(in crate::reactor) fn new(
        broker_limits: BrokerLimits,
        directory_limits: BrokerDirectoryLimits,
    ) -> Result<Self, BrokerSetError> {
        let capacity = directory_limits
            .max_brokers()
            .get()
            .checked_add(1)
            .and_then(NonZeroUsize::new)
            .ok_or(BrokerSetError::OwnerCapacityOverflow)?;
        Ok(Self {
            seed: None,
            directory: None,
            broker_limits,
            owner_capacity: capacity,
        })
    }

    pub(in crate::reactor) fn install_seed(
        &mut self,
        config: BrokerConfig,
        poller: &Poller,
        now: Moment,
    ) -> Result<(), BrokerSetError> {
        if self.seed.is_some() {
            return Err(BrokerSetError::SeedAlreadyInstalled);
        }
        let namespace = ResourceNamespace::new(0, self.owner_capacity)
            .ok_or(BrokerSetError::NamespaceUnavailable)?;
        let mut seed = SingleBroker::new_configured_in(config, self.broker_limits, namespace);
        seed.start(poller, now).map_err(BrokerSetError::Broker)?;
        self.seed = Some(seed);
        Ok(())
    }

    pub(in crate::reactor) const fn has_seed(&self) -> bool {
        self.seed.is_some()
    }

    pub(in crate::reactor) fn install_directory(
        &mut self,
        directory: &BrokerDirectory,
    ) -> Result<bool, BrokerSetError> {
        let limit = self.owner_capacity.get() - 1;
        if directory.len() > limit {
            return Err(BrokerSetError::DirectoryCapacity {
                observed: directory.len(),
                limit,
            });
        }
        if self.directory_generation() == Some(directory.generation()) {
            return Ok(false);
        }
        self.directory = Some(directory.clone());
        Ok(true)
    }

    pub(in crate::reactor) fn directory_generation(&self) -> Option<MetadataGeneration> {
        self.directory.as_ref().map(BrokerDirectory::generation)
    }

    pub(in crate::reactor) fn advertised_brokers(&self) -> usize {
        self.directory.as_ref().map_or(0, BrokerDirectory::len)
    }

    pub(in crate::reactor) fn seed_mut(&mut self) -> Option<&mut SingleBroker> {
        self.seed.as_mut()
    }

    pub(in crate::reactor) fn submit_seed(
        &mut self,
        poller: &Poller,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<(), BrokerSetError> {
        let Some(seed) = &mut self.seed else {
            return Err(BrokerSetError::SeedMissing);
        };
        seed.submit(poller, request, now)
            .map_err(BrokerSetError::Broker)?;
        Ok(())
    }

    pub(in crate::reactor) fn observe(
        &mut self,
        poller: &Poller,
        event: PollEvent,
        now: Moment,
    ) -> Result<bool, BrokerSetError> {
        let PollEvent::Resource { token, .. } = event else {
            return Ok(false);
        };
        if token.owner(
            self.broker_limits.resource_capacity().get(),
            self.owner_capacity.get(),
        ) != Some(0)
        {
            return Ok(false);
        }
        self.seed.as_mut().map_or(Ok(false), |seed| {
            seed.observe(poller, event, now)
                .map_err(BrokerSetError::Broker)
        })
    }

    pub(in crate::reactor) fn continue_io(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<bool, BrokerSetError> {
        self.seed.as_mut().map_or(Ok(false), |seed| {
            seed.continue_io(poller, now)
                .map_err(BrokerSetError::Broker)
        })
    }

    pub(in crate::reactor) fn fire_due(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<DeadlineProgress, BrokerSetError> {
        self.seed
            .as_mut()
            .map_or(Ok(DeadlineProgress::idle()), |seed| {
                seed.fire_due(poller, now).map_err(BrokerSetError::Broker)
            })
    }

    pub(in crate::reactor) fn next_deadline(&self) -> Option<Moment> {
        self.seed.as_ref().and_then(SingleBroker::next_deadline)
    }

    pub(in crate::reactor) fn begin_drain(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<(), BrokerSetError> {
        self.seed.as_mut().map_or(Ok(()), |seed| {
            seed.begin_drain(poller, now)
                .map_err(BrokerSetError::Broker)
        })
    }

    pub(in crate::reactor) fn is_terminal(&self) -> bool {
        self.seed.as_ref().is_none_or(SingleBroker::is_terminal)
    }

    pub(in crate::reactor) fn has_local_io(&self) -> bool {
        self.seed.as_ref().is_some_and(SingleBroker::has_local_io)
    }

    pub(in crate::reactor) fn seed_broker_state(&self) -> Option<BrokerState> {
        self.seed.as_ref().map(SingleBroker::broker_state)
    }

    pub(in crate::reactor) fn seed_connection_state(&self) -> Option<ConnectionState> {
        self.seed.as_ref().map(SingleBroker::state)
    }

    #[cfg(test)]
    pub(super) const fn owner_capacity(&self) -> NonZeroUsize {
        self.owner_capacity
    }
}
