//! Poll, timer, continuation, and shutdown delegation across broker owners.

use kafka_driver_core::Moment;

use crate::reactor::{
    PollEvent, Poller,
    broker::{DeadlineProgress, SingleBroker},
};

use super::{BrokerSet, BrokerSetError};

impl BrokerSet {
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
}
