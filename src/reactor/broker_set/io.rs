//! Poll, timer, continuation, and shutdown delegation across broker owners.

use kafka_driver_core::{CallFailure, Delivery, Moment, OutcomeStamp};

use crate::{
    RequestError,
    reactor::{
        PollEvent, Poller,
        broker::{DeadlineProgress, SingleBroker},
    },
};

use super::{BrokerSet, BrokerSetError};

impl BrokerSet {
    pub(in crate::reactor) fn observe(
        &mut self,
        poller: &Poller,
        event: PollEvent,
        now: Moment,
        observed_at: OutcomeStamp,
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
                seed.observe(poller, event, now, observed_at)
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
        let Some(child) = self.children.get_mut(index) else {
            return Ok(false);
        };
        let lane = child.lane();
        let progress = child.observe(poller, event, now, observed_at)?;
        self.sync_lane(lane)?;
        Ok(progress)
    }

    pub(in crate::reactor) fn continue_io(
        &mut self,
        poller: &Poller,
        now: Moment,
        observed_at: OutcomeStamp,
    ) -> Result<bool, BrokerSetError> {
        let mut progress = self.seed.as_mut().map_or(Ok(false), |seed| {
            seed.continue_io(poller, now, observed_at)
                .map_err(BrokerSetError::Broker)
        })?;
        progress |= self.admit_seed_waiting(poller, now)?;
        progress |= self.continue_runnable_lanes(poller, now, observed_at)?;
        Ok(progress)
    }

    pub(in crate::reactor) fn fire_due(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<DeadlineProgress, BrokerSetError> {
        let seed_waiting = self.seed_waiting.expire_due(now);
        let mut progress =
            DeadlineProgress::from_work(seed_waiting.settled(), seed_waiting.more_due());
        progress = progress.merge(
            self.seed
                .as_mut()
                .map_or(Ok(DeadlineProgress::idle()), |seed| {
                    seed.fire_due(poller, now).map_err(BrokerSetError::Broker)
                })?,
        );
        let mut progressed_lanes = 0;
        while progressed_lanes < self.lane_turn_budget.get() {
            let Some(lane) = self.deadlines.take_due(now) else {
                break;
            };
            let Some(index) = self.child_index(lane) else {
                continue;
            };
            let child = self
                .children
                .get_mut(index)
                .ok_or(BrokerSetError::UnknownBrokerChild)?;
            progress = progress.merge(child.fire_due(poller, now)?);
            self.sync_lane(lane)?;
            progressed_lanes += 1;
        }
        let more_due = self.deadlines.next_deadline().is_some_and(|at| at <= now);
        progress = progress.merge(DeadlineProgress::from_work(0, more_due));
        Ok(progress)
    }

    pub(in crate::reactor) fn next_deadline(&self) -> Option<Moment> {
        self.deadlines
            .next_deadline()
            .into_iter()
            .chain(self.seed_waiting.next_deadline())
            .chain(self.seed.as_ref().and_then(SingleBroker::next_deadline))
            .min()
    }

    pub(in crate::reactor) fn begin_drain(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<(), BrokerSetError> {
        self.seed_waiting.fail_all(&draining());
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
            let lane = child.lane();
            child.begin_drain(poller, now)?;
            self.sync_lane(lane)?;
            position += 1;
        }
        Ok(())
    }

    pub(in crate::reactor) fn is_terminal(&self) -> bool {
        self.seed_waiting.is_empty()
            && self.seed.as_ref().is_none_or(SingleBroker::is_terminal)
            && self
                .active_slots
                .iter()
                .filter_map(|index| self.children.get(*index))
                .all(|child| child.is_terminal())
    }

    pub(in crate::reactor) fn has_local_io(&self) -> bool {
        self.seed.as_ref().is_some_and(SingleBroker::has_local_io)
            || self.seed_waiting_has_local_work()
            || !self.address_refreshes.is_empty()
            || !self.runnable_lanes.is_empty()
    }
}

fn draining() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Draining,
        delivery: Delivery::NotSent,
    }
}
