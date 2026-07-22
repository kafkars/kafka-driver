//! Poll, timer, continuation, and shutdown delegation across broker owners.

use kafka_driver_core::Moment;

use crate::reactor::{
    PollEvent, Poller,
    broker::{DeadlineProgress, SingleBroker},
};

use super::{BrokerSet, BrokerSetError, child::BrokerChild};

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
        ) == Some(0)
        {
            return self.seed.as_mut().map_or(Ok(false), |seed| {
                seed.observe(poller, event, now)
                    .map_err(BrokerSetError::Broker)
            });
        }
        let Some(owner) = token.owner(
            self.broker_limits.resource_capacity().get(),
            self.owner_capacity.get(),
        ) else {
            return Ok(false);
        };
        let Some(index) = owner.checked_sub(1) else {
            return Ok(false);
        };
        self.children
            .get_mut(index)
            .and_then(Option::as_mut)
            .map_or(Ok(false), |child| child.observe(poller, event, now))
    }

    pub(in crate::reactor) fn continue_io(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<bool, BrokerSetError> {
        let mut progress = self.seed.as_mut().map_or(Ok(false), |seed| {
            seed.continue_io(poller, now)
                .map_err(BrokerSetError::Broker)
        })?;
        for child in self.children.iter_mut().flatten() {
            progress |= child.continue_io(poller, now)?;
        }
        progress |= self.activate_pending(poller, now)?;
        progress |= self.admit_waiting(poller, now)?;
        Ok(progress)
    }

    pub(in crate::reactor) fn fire_due(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<DeadlineProgress, BrokerSetError> {
        let mut progress = self
            .seed
            .as_mut()
            .map_or(Ok(DeadlineProgress::idle()), |seed| {
                seed.fire_due(poller, now).map_err(BrokerSetError::Broker)
            })?;
        for child in self.children.iter_mut().flatten() {
            progress = progress.merge(child.fire_due(poller, now)?);
        }
        Ok(progress)
    }

    pub(in crate::reactor) fn next_deadline(&self) -> Option<Moment> {
        self.children
            .iter()
            .filter_map(Option::as_ref)
            .filter_map(BrokerChild::next_deadline)
            .chain(self.seed.as_ref().and_then(SingleBroker::next_deadline))
            .min()
    }

    pub(in crate::reactor) fn begin_drain(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<(), BrokerSetError> {
        self.seed.as_mut().map_or(Ok(()), |seed| {
            seed.begin_drain(poller, now)
                .map_err(BrokerSetError::Broker)
        })?;
        for child in self.children.iter_mut().flatten() {
            child.begin_drain(poller, now)?;
        }
        Ok(())
    }

    pub(in crate::reactor) fn is_terminal(&self) -> bool {
        self.seed.as_ref().is_none_or(SingleBroker::is_terminal)
            && self
                .children
                .iter()
                .filter_map(Option::as_ref)
                .all(BrokerChild::is_terminal)
    }

    pub(in crate::reactor) fn has_local_io(&self) -> bool {
        self.seed.as_ref().is_some_and(SingleBroker::has_local_io)
            || self
                .children
                .iter()
                .filter_map(Option::as_ref)
                .any(BrokerChild::has_local_io)
    }

    fn admit_waiting(&mut self, poller: &Poller, now: Moment) -> Result<bool, BrokerSetError> {
        let mut progress = false;
        let mut admitted = 0;
        let mut idle_slots = 0;
        while admitted < self.admission_budget.get() && idle_slots < self.children.len() {
            let index = self.admission_cursor;
            self.admission_cursor = (self.admission_cursor + 1) % self.children.len();
            let made_progress = match self.children[index].as_mut() {
                Some(child) => child.admit_one(poller, now)?,
                None => false,
            };
            if made_progress {
                progress = true;
                admitted += 1;
                idle_slots = 0;
            } else {
                idle_slots += 1;
            }
        }
        Ok(progress)
    }
}
