//! FIFO, byte, deadline, and opt-in failure ownership before physical admission.

use std::num::NonZeroUsize;

use kafka_driver_core::{CallFailure, Delivery, Moment, OutcomeStamp};

use crate::{RequestError, reactor::wait_queue::WaitQueue, request::ErasedRequest};

pub(in crate::reactor) struct RouteWaiting {
    calls: WaitQueue<WaitingCall>,
    retained_bytes: usize,
    rejecting_calls: usize,
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
            rejecting_calls: 0,
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
        let rejecting = usize::from(request.rejects_after_route_failure());
        let waiting = WaitingCall { request, bytes };
        if let Err(waiting) = self.calls.push(waiting, deadline) {
            self.reject_capacity(waiting.request);
            return false;
        }
        self.retained_bytes = retained_bytes;
        self.rejecting_calls += rejecting;
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
        let request = self.release(waiting);
        let Some(remaining) = deadline.duration_since(now) else {
            fail(request, deadline_exceeded(), observed_at);
            return RouteWaitingOutcome::Settled;
        };
        if remaining.is_zero() {
            fail(request, deadline_exceeded(), observed_at);
            return RouteWaitingOutcome::Settled;
        }
        RouteWaitingOutcome::Ready(request)
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
            fail(self.release(waiting), deadline_exceeded(), observed_at);
            settled += 1;
        }
        RouteWaitingExpiration { settled }
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
            fail(self.release(waiting), failure.clone(), observed_at);
            settled += 1;
        }
        settled
    }

    pub(in crate::reactor) const fn has_failure_rejections(&self) -> bool {
        self.rejecting_calls != 0
    }

    /// Charges one examination even when a default-policy survivor is skipped.
    pub(in crate::reactor) fn reject_failed_route_one(
        &mut self,
        now: Moment,
        observed_at: OutcomeStamp,
    ) -> bool {
        if !self.has_failure_rejections() {
            return false;
        }
        if let Some((waiting, deadline)) = self
            .calls
            .scan_one(|waiting| waiting.request.rejects_after_route_failure())
        {
            let failure = if deadline <= now {
                deadline_exceeded()
            } else {
                RequestError::Rejected {
                    failure: CallFailure::NotReady,
                    delivery: Delivery::NotSent,
                }
            };
            fail(self.release(waiting), failure, Some(observed_at));
        }
        true
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

    fn release(&mut self, waiting: WaitingCall) -> Box<dyn ErasedRequest> {
        self.retained_bytes -= waiting.bytes;
        self.rejecting_calls -= usize::from(waiting.request.rejects_after_route_failure());
        waiting.request
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
}

impl RouteWaitingExpiration {
    pub(in crate::reactor) const fn settled(&self) -> usize {
        self.settled
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
