//! FIFO ownership for calls waiting on one semantic broker route.

use std::num::NonZeroUsize;

use kafka_driver_core::{CallFailure, Delivery, Moment, OutcomeStamp};

use crate::{RequestError, reactor::wait_queue::WaitQueue, request::ErasedRequest};

#[cfg(test)]
#[path = "route_waiting_legacy_test.rs"]
mod legacy;
#[cfg(test)]
pub(in crate::reactor) use legacy::terminal;

pub(in crate::reactor) struct RouteWaiting {
    calls: WaitQueue<WaitingCall>,
    retained_bytes: usize,
    call_limit: NonZeroUsize,
    byte_limit: NonZeroUsize,
    turn_budget: NonZeroUsize,
}

impl RouteWaiting {
    pub(in crate::reactor) fn new(
        call_limit: NonZeroUsize,
        byte_limit: NonZeroUsize,
        turn_budget: NonZeroUsize,
    ) -> Self {
        Self {
            calls: WaitQueue::new(call_limit),
            retained_bytes: 0,
            call_limit,
            byte_limit,
            turn_budget,
        }
    }

    pub(in crate::reactor) fn admit(
        &mut self,
        mut request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> bool {
        let deadline = match request.establish_deadline(now) {
            Ok(deadline) => deadline,
            Err(failure) => {
                request.fail(failure);
                return false;
            }
        };
        if deadline <= now {
            request.fail(deadline_exceeded());
            return false;
        }
        let bytes = request.retained_bytes();
        let Some(retained_bytes) = self.retained_bytes.checked_add(bytes) else {
            self.reject_capacity(request);
            return false;
        };
        if self.calls.len() == self.call_limit.get() || retained_bytes > self.byte_limit.get() {
            self.reject_capacity(request);
            return false;
        }
        let waiting = WaitingCall { request, bytes };
        if let Err(waiting) = self.calls.push(waiting, deadline) {
            self.reject_capacity(waiting.request);
            return false;
        }
        self.retained_bytes = retained_bytes;
        true
    }

    pub(in crate::reactor) fn pop(
        &mut self,
        now: Moment,
        observed_at: Option<OutcomeStamp>,
    ) -> RouteWaitingOutcome {
        let Some((waiting, deadline)) = self.calls.pop_front() else {
            return RouteWaitingOutcome::Empty;
        };
        self.retained_bytes -= waiting.bytes;
        let Some(remaining) = deadline.duration_since(now) else {
            fail(waiting.request, deadline_exceeded(), observed_at);
            return RouteWaitingOutcome::Settled;
        };
        if remaining.is_zero() {
            fail(waiting.request, deadline_exceeded(), observed_at);
            return RouteWaitingOutcome::Settled;
        }
        RouteWaitingOutcome::Ready(waiting.request)
    }

    #[cfg(test)]
    pub(in crate::reactor) fn expire_due(
        &mut self,
        now: Moment,
        observed_at: Option<OutcomeStamp>,
    ) -> RouteWaitingExpiration {
        self.expire_due_bounded(now, observed_at, self.turn_budget.get())
    }

    pub(in crate::reactor) fn expire_due_bounded(
        &mut self,
        now: Moment,
        observed_at: Option<OutcomeStamp>,
        budget: usize,
    ) -> RouteWaitingExpiration {
        let mut settled = 0;
        let budget = budget.min(self.turn_budget.get());
        while settled < budget {
            let Some((waiting, _)) = self.calls.take_due(now) else {
                break;
            };
            self.retained_bytes -= waiting.bytes;
            fail(waiting.request, deadline_exceeded(), observed_at);
            settled += 1;
        }
        #[cfg(test)]
        let more_due = self
            .calls
            .next_deadline()
            .is_some_and(|deadline| deadline <= now);
        RouteWaitingExpiration {
            settled,
            #[cfg(test)]
            more_due,
        }
    }

    pub(in crate::reactor) fn next_deadline(&self) -> Option<Moment> {
        self.calls.next_deadline()
    }

    pub(in crate::reactor) fn fail_all(
        &mut self,
        failure: &RequestError,
        observed_at: Option<OutcomeStamp>,
    ) {
        let _ = self.fail_bounded(failure, observed_at, usize::MAX);
    }

    pub(in crate::reactor) fn fail_bounded(
        &mut self,
        failure: &RequestError,
        observed_at: Option<OutcomeStamp>,
        budget: usize,
    ) -> usize {
        let mut settled = 0;
        while settled < budget {
            let Some((waiting, _)) = self.calls.pop_front() else {
                break;
            };
            self.retained_bytes -= waiting.bytes;
            fail(waiting.request, failure.clone(), observed_at);
            settled += 1;
        }
        settled
    }

    pub(in crate::reactor) fn is_empty(&self) -> bool {
        self.calls.is_empty()
    }

    pub(in crate::reactor) fn has_live_after(&self, now: Moment) -> bool {
        self.calls.iter().any(|(_, deadline)| deadline > now)
    }

    pub(in crate::reactor) fn len(&self) -> usize {
        self.calls.len()
    }

    pub(in crate::reactor) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn reject_capacity(&self, request: Box<dyn ErasedRequest>) {
        request.fail(RequestError::RouteCapacityReached {
            call_limit: self.call_limit.get(),
            byte_limit: self.byte_limit.get(),
        });
    }
}

pub(in crate::reactor) enum RouteWaitingOutcome {
    Empty,
    Settled,
    Ready(Box<dyn ErasedRequest>),
}

pub(in crate::reactor) struct RouteWaitingExpiration {
    settled: usize,
    #[cfg(test)]
    more_due: bool,
}

impl RouteWaitingExpiration {
    pub(in crate::reactor) const fn settled(&self) -> usize {
        self.settled
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn more_due(&self) -> bool {
        self.more_due
    }
}

struct WaitingCall {
    request: Box<dyn ErasedRequest>,
    bytes: usize,
}

fn deadline_exceeded() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::DeadlineExceeded,
        delivery: Delivery::NotSent,
    }
}

fn fail(request: Box<dyn ErasedRequest>, failure: RequestError, observed_at: Option<OutcomeStamp>) {
    match observed_at {
        Some(observed_at) => request.fail_observed(failure, observed_at),
        None => request.fail(failure),
    }
}
