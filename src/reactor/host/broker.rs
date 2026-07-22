//! Host-phase delegation to the currently installed broker connection owner.

use std::time::Duration;

use crate::reactor::{
    broker::{DeadlineProgress, SingleBroker},
    clock::ReactorClock,
};

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn observe_poll_events(&mut self) -> Result<bool, ReactorError> {
        let now = self.clock.now().map_err(ReactorError::clock)?;
        let Some(broker) = &mut self.broker else {
            self.poll_events.clear();
            return Ok(false);
        };
        let mut progress = false;
        for event in self.poll_events.drain(..) {
            progress |= broker
                .observe(&self.poller, event, now)
                .map_err(ReactorError::broker)?;
        }
        Ok(progress)
    }

    pub(super) fn continue_broker_io(&mut self) -> Result<bool, ReactorError> {
        let now = self.clock.now().map_err(ReactorError::clock)?;
        self.broker.as_mut().map_or(Ok(false), |broker| {
            broker
                .continue_io(&self.poller, now)
                .map_err(ReactorError::broker)
        })
    }

    pub(super) fn fire_due_deadlines(&mut self) -> Result<DeadlineProgress, ReactorError> {
        let now = self.clock.now().map_err(ReactorError::clock)?;
        self.broker
            .as_mut()
            .map_or(Ok(DeadlineProgress::idle()), |broker| {
                broker
                    .fire_due(&self.poller, now)
                    .map_err(ReactorError::broker)
            })
    }

    pub(super) fn poll_wait(&self, host_limit: Duration) -> Result<Duration, ReactorError> {
        let now = self.clock.now().map_err(ReactorError::clock)?;
        let deadline = self.broker.as_ref().and_then(SingleBroker::next_deadline);
        Ok(ReactorClock::bounded_wait(now, deadline, host_limit))
    }

    pub(super) fn broker_has_local_io(&self) -> bool {
        self.broker.as_ref().is_some_and(SingleBroker::has_local_io)
    }
}
