//! Fair routing and settlement of calls after coordinator discovery.

use kafka_driver_core::Moment;

use crate::RequestError;

use super::{
    CoordinatorOwner,
    waiting::{CoordinatorWaitProgress, RoutedCoordinatorCall, WaitingCoordinatorOutcome},
};

impl CoordinatorOwner {
    pub(in crate::reactor) fn drain_waiters(&mut self, now: Moment) -> CoordinatorWaitProgress {
        self.waiters.prepare_due_scan(now);
        let mut progress = CoordinatorWaitProgress::default();
        for _ in 0..self.limits.turn_budget().get() {
            match self.waiters.pop(now) {
                WaitingCoordinatorOutcome::Empty => break,
                WaitingCoordinatorOutcome::Settled => {
                    progress.examined += 1;
                    progress.settled += 1;
                }
                WaitingCoordinatorOutcome::Ready {
                    mut waiting,
                    remaining,
                } => {
                    progress.examined += 1;
                    if let Some(route) = self.current(&waiting.key).cloned() {
                        waiting.request.set_timeout(remaining);
                        progress.routed.push(RoutedCoordinatorCall {
                            route,
                            request: waiting.request,
                        });
                    } else if self.discovery_pending(&waiting.key) {
                        self.waiters.retain(waiting);
                    } else {
                        waiting.request.fail(RequestError::RouteUnavailable);
                        progress.settled += 1;
                    }
                }
            }
        }
        progress.more_work = self.waiters.has_pending_scan();
        progress
    }

    pub(in crate::reactor) fn next_wait_deadline(&self) -> Option<Moment> {
        self.waiters.next_deadline()
    }

    pub(in crate::reactor) fn has_local_work(&self) -> bool {
        self.waiters.has_pending_scan()
    }

    pub(in crate::reactor) fn fail_waiters(&mut self, failure: &RequestError) {
        self.waiters.fail_all(failure);
    }
}
