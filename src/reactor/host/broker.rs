//! Host-phase delegation to the currently installed broker connection owner.

use std::time::Duration;

use kafka_driver_core::Moment;

use crate::reactor::{broker::DeadlineProgress, direct_plaintext::DirectBackend};

use super::{Reactor, ReactorError};

const WORKER_SHUTDOWN_OBSERVATION_INTERVAL: Duration = Duration::from_millis(10);

impl Reactor {
    pub(super) fn observe_poll_events(&mut self, now: Moment) -> Result<bool, ReactorError> {
        let Some(legacy) = self.backend.legacy_mut() else {
            return Ok(false);
        };
        let mut events = std::mem::take(&mut legacy.poll_events);
        let mut progress = false;
        for event in events.drain(..) {
            let observed_at = self.causality.outcome().map_err(ReactorError::causality)?;
            progress |= legacy
                .brokers
                .observe(&legacy.poller, event, now, observed_at)
                .map_err(ReactorError::broker_set)?;
        }
        legacy.poll_events = events;
        Ok(progress)
    }

    pub(super) fn continue_broker_io(&mut self, now: Moment) -> Result<bool, ReactorError> {
        if let Some(direct) = self.backend.direct_mut() {
            return direct
                .drive(now, &mut self.causality)
                .map_err(ReactorError::host);
        }
        let observed_at = self.causality.outcome().map_err(ReactorError::causality)?;
        let Some(legacy) = self.backend.legacy_mut() else {
            return Ok(false);
        };
        legacy
            .brokers
            .continue_io(&legacy.poller, now, observed_at)
            .map_err(ReactorError::broker_set)
    }

    pub(super) fn fire_due_deadlines(
        &mut self,
        now: Moment,
    ) -> Result<DeadlineProgress, ReactorError> {
        let Some(legacy) = self.backend.legacy_mut() else {
            return Ok(DeadlineProgress::idle());
        };
        legacy
            .brokers
            .fire_due(&legacy.poller, now)
            .map_err(ReactorError::broker_set)
    }

    pub(super) fn next_deadline(&self, now: Moment) -> Option<Moment> {
        let backend = self.backend.legacy().map_or_else(
            || self.backend.direct().and_then(DirectBackend::next_deadline),
            |legacy| legacy.brokers.next_deadline(),
        );
        backend
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
        self.backend.legacy().map_or_else(
            || {
                self.backend
                    .direct()
                    .is_some_and(DirectBackend::has_local_work)
            },
            |legacy| legacy.brokers.has_local_io(),
        )
    }
}
