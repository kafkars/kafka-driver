//! Host-phase delegation to the currently installed broker connection owner.

use std::time::Duration;

use kafka_driver_core::Moment;

use crate::reactor::broker::DeadlineProgress;

use super::{Reactor, ReactorError};

const WORKER_SHUTDOWN_OBSERVATION_INTERVAL: Duration = Duration::from_millis(10);

impl Reactor {
    pub(super) fn observe_poll_events(&mut self, now: Moment) -> Result<bool, ReactorError> {
        let mut progress = false;
        for event in self.poll_events.drain(..) {
            let observed_at = self.causality.outcome().map_err(ReactorError::causality)?;
            progress |= self
                .brokers
                .observe(&self.poller, event, now, observed_at)
                .map_err(ReactorError::broker_set)?;
        }
        Ok(progress)
    }

    pub(super) fn continue_broker_io(&mut self, now: Moment) -> Result<bool, ReactorError> {
        let observed_at = self.causality.outcome().map_err(ReactorError::causality)?;
        self.brokers
            .continue_io(&self.poller, now, observed_at)
            .map_err(ReactorError::broker_set)
    }

    pub(super) fn fire_due_deadlines(
        &mut self,
        now: Moment,
    ) -> Result<DeadlineProgress, ReactorError> {
        self.brokers
            .fire_due(&self.poller, now)
            .map_err(ReactorError::broker_set)
    }

    pub(super) fn next_deadline(&self, now: Moment) -> Option<Moment> {
        self.brokers
            .next_deadline()
            .into_iter()
            .chain(
                self.resolution
                    .as_ref()
                    .and_then(super::resolution::NameResolution::next_deadline),
            )
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
            .chain(
                self.worker_shutdown_pending()
                    .then(|| now.checked_add(WORKER_SHUTDOWN_OBSERVATION_INTERVAL))
                    .flatten(),
            )
            .min()
    }

    pub(super) fn broker_has_local_io(&self) -> bool {
        self.brokers.has_local_io()
    }
}
