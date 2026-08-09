//! Seed-connection installation, submission, and diagnostics.

use kafka_driver_core::{
    BrokerPhase, BrokerState, ConnectionPhase, ConnectionState, Moment, OutcomeStamp,
};

use crate::{
    config::BrokerConfig,
    reactor::{Poller, bootstrap::ResolvedSeed, broker::SingleBroker, resource::ResourceNamespace},
    request::ErasedRequest,
};

use super::{
    BrokerSet, BrokerSetError,
    waiting::{WaitingCallOutcome, terminal},
};

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
        if let Some(BrokerState::Closed { reason }) = self.seed_broker_state() {
            request.fail(terminal(reason));
            return Ok(());
        }
        if self.seed_is_ready() {
            let Some(seed) = &mut self.seed else {
                return Err(BrokerSetError::SeedMissing);
            };
            return seed
                .submit(poller, request, now)
                .map_err(BrokerSetError::Broker);
        }
        self.seed_waiting.admit(request, now);
        Ok(())
    }

    pub(super) fn admit_seed_waiting(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<bool, BrokerSetError> {
        if !self.seed_is_ready() {
            return Ok(false);
        }
        let mut progress = false;
        for _admission in 0..self.admission_budget.get() {
            match self.seed_waiting.pop(now, None) {
                WaitingCallOutcome::Empty => break,
                WaitingCallOutcome::Settled => progress = true,
                WaitingCallOutcome::Ready(request) => {
                    let Some(seed) = &mut self.seed else {
                        return Err(BrokerSetError::SeedMissing);
                    };
                    seed.submit(poller, request, now)
                        .map_err(BrokerSetError::Broker)?;
                    progress = true;
                }
            }
        }
        Ok(progress)
    }

    pub(super) fn seed_waiting_has_local_work(&self) -> bool {
        self.seed_is_ready() && !self.seed_waiting.is_empty()
    }

    pub(super) fn settle_terminal_seed_waiting(
        &mut self,
        observed_at: Option<OutcomeStamp>,
    ) -> usize {
        let Some(BrokerState::Closed { reason }) = self.seed_broker_state() else {
            return 0;
        };
        let settled = self.seed_waiting.len();
        if settled != 0 {
            self.seed_waiting.fail_all(&terminal(reason), observed_at);
        }
        settled
    }

    fn seed_is_ready(&self) -> bool {
        self.seed.as_ref().is_some_and(|seed| {
            seed.state().phase() == ConnectionPhase::Ready
                && seed.broker_state().phase() == BrokerPhase::Available
        })
    }

    pub(in crate::reactor) fn seed_broker_state(&self) -> Option<BrokerState> {
        self.seed.as_ref().map(SingleBroker::broker_state)
    }

    pub(in crate::reactor) fn seed_connection_state(&self) -> Option<ConnectionState> {
        self.seed.as_ref().map(SingleBroker::state)
    }
}
