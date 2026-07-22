//! Host-phase delegation to the currently installed broker connection owner.

use std::time::Duration;

use crate::reactor::{broker::DeadlineProgress, clock::ReactorClock};

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn observe_poll_events(&mut self) -> Result<bool, ReactorError> {
        let now = self.clock.now().map_err(ReactorError::clock)?;
        let mut progress = false;
        for event in self.poll_events.drain(..) {
            progress |= self
                .brokers
                .observe(&self.poller, event, now)
                .map_err(ReactorError::broker_set)?;
        }
        Ok(progress)
    }

    pub(super) fn continue_broker_io(&mut self) -> Result<bool, ReactorError> {
        let now = self.clock.now().map_err(ReactorError::clock)?;
        self.brokers
            .continue_io(&self.poller, now)
            .map_err(ReactorError::broker_set)
    }

    pub(super) fn fire_due_deadlines(&mut self) -> Result<DeadlineProgress, ReactorError> {
        let now = self.clock.now().map_err(ReactorError::clock)?;
        self.brokers
            .fire_due(&self.poller, now)
            .map_err(ReactorError::broker_set)
    }

    pub(super) fn poll_wait(&self, host_limit: Duration) -> Result<Duration, ReactorError> {
        let now = self.clock.now().map_err(ReactorError::clock)?;
        let deadline = self
            .brokers
            .next_deadline()
            .into_iter()
            .chain(
                self.metadata
                    .as_ref()
                    .and_then(super::super::metadata::MetadataOwner::next_wait_deadline),
            )
            .chain(
                self.coordinator
                    .as_ref()
                    .and_then(super::super::coordinator::CoordinatorOwner::next_wait_deadline),
            )
            .min();
        Ok(ReactorClock::bounded_wait(now, deadline, host_limit))
    }

    pub(super) fn broker_has_local_io(&self) -> bool {
        self.brokers.has_local_io()
    }
}
