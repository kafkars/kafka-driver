//! FIFO ownership for calls waiting on one lazily opened broker connection.

use std::num::NonZeroUsize;

use kafka_driver_core::{CallFailure, Delivery, Moment, OutcomeStamp};

use crate::{RequestError, reactor::wait_queue::WaitQueue, request::ErasedRequest};

pub(super) struct WaitingCalls {
    calls: WaitQueue<WaitingCall>,
    retained_bytes: usize,
    call_limit: NonZeroUsize,
    byte_limit: NonZeroUsize,
    turn_budget: NonZeroUsize,
}

impl WaitingCalls {
    pub(super) fn new(
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

    pub(super) fn admit(&mut self, mut request: Box<dyn ErasedRequest>, now: Moment) -> bool {
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

    pub(super) fn pop(
        &mut self,
        now: Moment,
        observed_at: Option<OutcomeStamp>,
    ) -> WaitingCallOutcome {
        let Some((waiting, deadline)) = self.calls.pop_front() else {
            return WaitingCallOutcome::Empty;
        };
        self.retained_bytes -= waiting.bytes;
        let Some(remaining) = deadline.duration_since(now) else {
            fail(waiting.request, deadline_exceeded(), observed_at);
            return WaitingCallOutcome::Settled;
        };
        if remaining.is_zero() {
            fail(waiting.request, deadline_exceeded(), observed_at);
            return WaitingCallOutcome::Settled;
        }
        WaitingCallOutcome::Ready(waiting.request)
    }

    pub(super) fn expire_due(
        &mut self,
        now: Moment,
        observed_at: Option<OutcomeStamp>,
    ) -> WaitingExpiration {
        let mut settled = 0;
        while settled < self.turn_budget.get() {
            let Some((waiting, _)) = self.calls.take_due(now) else {
                break;
            };
            self.retained_bytes -= waiting.bytes;
            fail(waiting.request, deadline_exceeded(), observed_at);
            settled += 1;
        }
        let more_due = self
            .calls
            .next_deadline()
            .is_some_and(|deadline| deadline <= now);
        WaitingExpiration { settled, more_due }
    }

    pub(super) fn next_deadline(&self) -> Option<Moment> {
        self.calls.next_deadline()
    }

    pub(super) fn fail_all(&mut self, failure: &RequestError, observed_at: Option<OutcomeStamp>) {
        for waiting in self.calls.drain() {
            fail(waiting.request, failure.clone(), observed_at);
        }
        self.retained_bytes = 0;
    }

    pub(super) fn is_empty(&self) -> bool {
        self.calls.is_empty()
    }

    pub(super) fn len(&self) -> usize {
        self.calls.len()
    }

    pub(super) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn reject_capacity(&self, request: Box<dyn ErasedRequest>) {
        request.fail(RequestError::RouteCapacityReached {
            call_limit: self.call_limit.get(),
            byte_limit: self.byte_limit.get(),
        });
    }
}

pub(super) enum WaitingCallOutcome {
    Empty,
    Settled,
    Ready(Box<dyn ErasedRequest>),
}

pub(super) struct WaitingExpiration {
    settled: usize,
    more_due: bool,
}

impl WaitingExpiration {
    pub(super) const fn settled(&self) -> usize {
        self.settled
    }

    pub(super) const fn more_due(&self) -> bool {
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
