//! Seed-connection installation, submission, and diagnostics.

use kafka_driver_core::{BrokerState, ConnectionState, Moment};

use crate::{
    config::BrokerConfig,
    reactor::{Poller, bootstrap::ResolvedSeed, broker::SingleBroker, resource::ResourceNamespace},
    request::ErasedRequest,
};

use super::{BrokerSet, BrokerSetError};

impl BrokerSet {
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
        let mut seed = SingleBroker::new_configured_in(
            config,
            self.broker_limits,
            namespace,
            self.scram_proof.clone(),
        );
        seed.start(poller, now).map_err(BrokerSetError::Broker)?;
        self.seed = Some(seed);
        Ok(())
    }

    pub(in crate::reactor) fn install_resolved_seed(
        &mut self,
        seed: ResolvedSeed,
        poller: &Poller,
        now: Moment,
    ) -> Result<(), BrokerSetError> {
        let generation = seed.generation();
        self.install_seed(seed.into_config(), poller, now)?;
        self.seed_generation = Some(generation);
        Ok(())
    }

    pub(in crate::reactor) const fn has_seed(&self) -> bool {
        self.seed.is_some()
    }

    pub(in crate::reactor) fn seed_mut(&mut self) -> Option<&mut SingleBroker> {
        self.seed.as_mut()
    }

    pub(in crate::reactor) fn take_seed_address_refresh(
        &mut self,
    ) -> Result<Option<kafka_driver_core::BrokerEndpoint>, BrokerSetError> {
        let Some(seed) = self.seed.as_mut() else {
            return Ok(None);
        };
        seed.take_address_refresh().map_err(BrokerSetError::Broker)
    }

    pub(in crate::reactor) fn restore_seed_address_refresh(
        &mut self,
    ) -> Result<(), BrokerSetError> {
        if let Some(seed) = &mut self.seed {
            seed.restore_address_refresh()
                .map_err(BrokerSetError::Broker)?;
        }
        Ok(())
    }

    pub(in crate::reactor) fn replace_seed_endpoint(
        &mut self,
        seed: ResolvedSeed,
        poller: &Poller,
        now: Moment,
    ) -> Result<bool, BrokerSetError> {
        let generation = seed.generation();
        let Some(current) = self.seed_generation else {
            return Err(BrokerSetError::UnexpectedResolutionEffect);
        };
        if generation <= current {
            return Ok(false);
        }
        let config = seed.into_config();
        let Some(current_seed) = &mut self.seed else {
            return Err(BrokerSetError::SeedMissing);
        };
        if !config.is_resolved() {
            return Err(BrokerSetError::UnexpectedResolutionEffect);
        }
        current_seed
            .replace_exhausted_endpoint(config, poller, now)
            .map_err(BrokerSetError::Broker)?;
        self.seed_generation = Some(generation);
        Ok(true)
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
            .map_err(BrokerSetError::Broker)
    }

    pub(in crate::reactor) fn seed_broker_state(&self) -> Option<BrokerState> {
        self.seed.as_ref().map(SingleBroker::broker_state)
    }

    pub(in crate::reactor) fn seed_connection_state(&self) -> Option<ConnectionState> {
        self.seed.as_ref().map(SingleBroker::state)
    }
}
