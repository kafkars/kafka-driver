//! FIFO ownership for calls waiting on one lazily opened broker connection.

use std::{collections::VecDeque, num::NonZeroUsize};

use kafka_driver_core::{CallFailure, Delivery, Moment};

use crate::{RequestError, request::ErasedRequest};

pub(super) struct WaitingCalls {
    calls: VecDeque<WaitingCall>,
    retained_bytes: usize,
    call_limit: NonZeroUsize,
    byte_limit: NonZeroUsize,
}

impl WaitingCalls {
    pub(super) fn new(call_limit: NonZeroUsize, byte_limit: NonZeroUsize) -> Self {
        Self {
            calls: VecDeque::with_capacity(call_limit.get().min(16)),
            retained_bytes: 0,
            call_limit,
            byte_limit,
        }
    }

    pub(super) fn admit(&mut self, request: Box<dyn ErasedRequest>, now: Moment) -> bool {
        let Some(deadline) = now.checked_add(request.timeout()) else {
            request.fail(RequestError::DeadlineOverflow);
            return false;
        };
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
        let Some(mut waiting) = self.calls.pop_front() else {
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
        waiting.request.set_timeout(remaining);
        WaitingCallOutcome::Ready(waiting.request)
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
