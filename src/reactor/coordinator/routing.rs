//! Fair routing and settlement of calls after coordinator discovery.

use kafka_driver_core::Moment;

use crate::RequestError;

use super::{
    CoordinatorOwner,
    waiting::WaitingCoordinatorOutcome,
    waiting_progress::{CoordinatorWaitProgress, RoutedCoordinatorCall},
};

impl CoordinatorOwner {
    pub(in crate::reactor) fn drain_waiters(&mut self, now: Moment) -> CoordinatorWaitProgress {
        let mut progress = CoordinatorWaitProgress::default();
        let budget = self.limits.turn_budget().get();
        let expired = self.waiters.expire_due(now, budget);
        progress.examined = expired;
        progress.settled = expired;
        for _ in expired..budget {
            match self.waiters.pop(now) {
                WaitingCoordinatorOutcome::Empty => break,
                WaitingCoordinatorOutcome::Settled => {
                    progress.examined += 1;
                    progress.settled += 1;
                }
                WaitingCoordinatorOutcome::Ready { waiting, deadline } => {
                    progress.examined += 1;
                    if let Some(route) = self.current(&waiting.key).cloned() {
                        progress.routed.push(RoutedCoordinatorCall {
                            route,
                            request: waiting.request,
                        });
                    } else if self.discovery_pending(&waiting.key) {
                        if !self.waiters.retain(waiting, deadline) {
                            progress.settled += 1;
                        }
                    } else {
                        waiting.request.fail(RequestError::RouteUnavailable);
                        progress.settled += 1;
                    }
                }
            }
        }
        self.waiters.refresh_due(now);
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
        self.fail_all_invalidations();
    }
}
