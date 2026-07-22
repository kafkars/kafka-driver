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
        let progress = self
            .children
            .get_mut(index)
            .map_or(Ok(false), |child| child.observe(poller, event, now))?;
        Ok(progress | self.reclaim_reusable_children()?)
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
        let mut position = 0;
        while let Some(index) = self.active_slots.get(position).copied() {
            let child = self
                .children
                .get_mut(index)
                .ok_or(BrokerSetError::UnknownBrokerChild)?;
            progress |= child.continue_io(poller, now)?;
            position += 1;
        }
        progress |= self.activate_pending(poller, now)?;
        progress |= self.admit_waiting(poller, now)?;
        progress |= self.reclaim_reusable_children()?;
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
        let mut position = 0;
        while let Some(index) = self.active_slots.get(position).copied() {
            let child = self
                .children
                .get_mut(index)
                .ok_or(BrokerSetError::UnknownBrokerChild)?;
            progress = progress.merge(child.fire_due(poller, now)?);
            position += 1;
        }
        progress = progress.merge(DeadlineProgress::from_work(
            usize::from(self.reclaim_reusable_children()?),
            false,
        ));
        Ok(progress)
    }

    pub(in crate::reactor) fn next_deadline(&self) -> Option<Moment> {
        self.active_slots
            .iter()
            .filter_map(|index| self.children.get(*index))
            .filter_map(|child| child.next_deadline())
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
        let mut position = 0;
        while let Some(index) = self.active_slots.get(position).copied() {
            let child = self
                .children
                .get_mut(index)
                .ok_or(BrokerSetError::UnknownBrokerChild)?;
            child.begin_drain(poller, now)?;
            position += 1;
        }
        Ok(())
    }

    pub(in crate::reactor) fn is_terminal(&self) -> bool {
        self.seed.as_ref().is_none_or(SingleBroker::is_terminal)
            && self
                .active_slots
                .iter()
                .filter_map(|index| self.children.get(*index))
                .all(|child| child.is_terminal())
    }

    pub(in crate::reactor) fn has_local_io(&self) -> bool {
        self.seed.as_ref().is_some_and(SingleBroker::has_local_io)
            || self
                .active_slots
                .iter()
                .filter_map(|index| self.children.get(*index))
                .any(|child| child.has_local_io())
    }

    fn admit_waiting(&mut self, poller: &Poller, now: Moment) -> Result<bool, BrokerSetError> {
        let mut progress = false;
        let mut admitted = 0;
        let mut idle_slots = 0;
        while admitted < self.admission_budget.get() && idle_slots < self.active_slots.len() {
            let slot_position = self.admission_cursor;
            self.admission_cursor = (self.admission_cursor + 1) % self.active_slots.len();
            let index = self.active_slots[slot_position];
            let made_progress = self
                .children
                .get_mut(index)
                .ok_or(BrokerSetError::UnknownBrokerChild)?
                .admit_one(poller, now)?;
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
