//! Bounded terminal settlement for seed-route ownership outside a Bornera lane.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{BrokerCloseReason, BrokerState, CallFailure, Delivery};

use crate::{RequestError, reactor::direct_plaintext::owner::DirectLane};

use super::ClusterRuntime;

#[cfg(test)]
#[path = "seed_waiting_settlement_test.rs"]
mod test;

#[cfg(test)]
#[path = "seed_waiting_fatal_test.rs"]
mod fatal_test;

pub(super) struct SeedWaitingState {
    queued_failure: Option<RequestError>,
    draining: bool,
}

impl SeedWaitingState {
    pub(super) const fn open() -> Self {
        Self {
            queued_failure: None,
            draining: false,
        }
    }

    fn queued_failure(&self) -> Option<RequestError> {
        self.queued_failure
            .clone()
            .or_else(|| self.draining.then(draining))
    }

    fn admission_failure(&self) -> Option<RequestError> {
        self.draining
            .then(draining)
            .or_else(|| self.queued_failure.clone())
    }

    const fn blocks_replacement(&self, has_waiters: bool) -> bool {
        self.draining || (self.queued_failure.is_some() && has_waiters)
    }

    fn reopen_after_replacement(&mut self) {
        self.queued_failure = None;
    }

    const fn is_open(&self) -> bool {
        !self.draining && self.queued_failure.is_none()
    }
}

impl<T: RegisteredTransport> ClusterRuntime<T> {
    /// Closes external admission without spending the next drive's work budget.
    pub(super) fn begin_seed_waiting_drain(&mut self) {
        let _ = self.capture_seed_terminal_failure();
        self.seed_waiting_state.draining = true;
    }

    pub(super) fn capture_seed_terminal_failure(&mut self) -> io::Result<()> {
        if !self.seed_waiting_state.is_open() {
            return Ok(());
        }
        let Some(index) = self.seed_lane_index()? else {
            return Ok(());
        };
        let Some(failure) = terminal_admission_failure(&self.lanes[index]) else {
            return Ok(());
        };
        self.seed_waiting_state.queued_failure = Some(failure);
        Ok(())
    }

    pub(super) fn seed_replacement_blocked(&mut self) -> io::Result<bool> {
        self.capture_seed_terminal_failure()?;
        Ok(self
            .seed_waiting_state
            .blocks_replacement(!self.seed_waiting.is_empty()))
    }

    pub(super) fn reopen_seed_waiting_after_replacement(&mut self) {
        self.seed_waiting_state.reopen_after_replacement();
    }

    pub(super) fn settle_failed_seed_waiting(&mut self, budget: usize) -> usize {
        let Some(failure) = self.seed_waiting_state.queued_failure() else {
            return 0;
        };
        self.seed_waiting.fail_bounded(&failure, budget)
    }

    fn totalize_seed_waiting_after_host_failure(&mut self) {
        if self.seed_waiting_state.queued_failure().is_none() {
            self.seed_waiting_state.queued_failure = Some(closed());
        }
        let Some(failure) = self.seed_waiting_state.queued_failure() else {
            return;
        };
        // The host exits after this error, so fatal ownership totality wins over
        // normal turn budgeting just as it does for lane contexts and pending calls.
        self.seed_waiting.fail_bounded(&failure, usize::MAX);
    }

    pub(super) fn finish_seed_host_result<R>(&mut self, result: io::Result<R>) -> io::Result<R> {
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                let _ = self.capture_seed_terminal_failure();
                self.totalize_seed_waiting_after_host_failure();
                Err(error)
            }
        }
    }

    pub(super) fn seed_waiting_is_closed(&self) -> bool {
        !self.seed_waiting_state.is_open()
    }

    pub(super) fn seed_waiting_admission_failure(&self) -> Option<RequestError> {
        self.seed_waiting_state.admission_failure()
    }

    pub(super) fn seed_waiting_has_local_work(&self) -> bool {
        if self.seed_waiting.is_empty() {
            return false;
        }
        if self.seed_waiting_is_closed() {
            return true;
        }
        match self.seed_lane_index() {
            Ok(Some(index)) => {
                self.lanes[index].can_admit_public()
                    || terminal_admission_failure(&self.lanes[index]).is_some()
            }
            Ok(None) => false,
            Err(_) => true,
        }
    }
}

fn terminal_admission_failure<T: RegisteredTransport>(
    lane: &DirectLane<T>,
) -> Option<RequestError> {
    // Requested closure also makes an old seed reclaimable for replacement.
    // Global shutdown installs Draining first; replacement preserves its waiters.
    if matches!(
        lane.lifecycle.state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::Requested
        }
    ) {
        return None;
    }
    lane.terminal_admission_failure()
}

fn draining() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Draining,
        delivery: Delivery::NotSent,
    }
}

fn closed() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Closed,
        delivery: Delivery::NotSent,
    }
}
