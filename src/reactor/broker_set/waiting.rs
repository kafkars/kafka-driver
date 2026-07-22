//! FIFO ownership for calls waiting on one lazily opened broker connection.

use std::{collections::VecDeque, num::NonZeroUsize};

use kafka_driver_core::{CallFailure, Delivery, Moment};

use crate::{RequestError, request::ErasedRequest};

pub(super) struct WaitingCalls {
    calls: VecDeque<WaitingCall>,
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
            calls: VecDeque::with_capacity(call_limit.get().min(16)),
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
        self.calls.push_back(WaitingCall {
            request,
            deadline,
            bytes,
        });
        self.retained_bytes = retained_bytes;
        true
    }

    pub(super) fn pop(&mut self, now: Moment) -> WaitingCallOutcome {
        let Some(waiting) = self.calls.pop_front() else {
            return WaitingCallOutcome::Empty;
        };
        self.retained_bytes -= waiting.bytes;
        let Some(remaining) = waiting.deadline.duration_since(now) else {
            waiting.request.fail(deadline_exceeded());
            return WaitingCallOutcome::Settled;
        };
        if remaining.is_zero() {
            waiting.request.fail(deadline_exceeded());
            return WaitingCallOutcome::Settled;
        }
        WaitingCallOutcome::Ready(waiting.request)
    }

    pub(super) fn expire_due(&mut self, now: Moment) -> WaitingExpiration {
        let mut survivors = VecDeque::with_capacity(self.calls.len());
        let mut settled = 0;
        let mut more_due = false;
        while let Some(waiting) = self.calls.pop_front() {
            if is_due(waiting.deadline, now) && settled < self.turn_budget.get() {
                self.retained_bytes -= waiting.bytes;
                waiting.request.fail(deadline_exceeded());
                settled += 1;
            } else {
                more_due |= is_due(waiting.deadline, now);
                survivors.push_back(waiting);
            }
        }
        self.calls = survivors;
        WaitingExpiration { settled, more_due }
    }

    pub(super) fn next_deadline(&self) -> Option<Moment> {
        self.calls.iter().map(|waiting| waiting.deadline).min()
    }

    pub(super) fn fail_all(&mut self, failure: &RequestError) {
        for waiting in self.calls.drain(..) {
            waiting.request.fail(failure.clone());
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
    deadline: Moment,
    bytes: usize,
}

fn deadline_exceeded() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::DeadlineExceeded,
        delivery: Delivery::NotSent,
    }
}

fn is_due(deadline: Moment, now: Moment) -> bool {
    deadline
        .duration_since(now)
        .is_none_or(|remaining| remaining.is_zero())
}
