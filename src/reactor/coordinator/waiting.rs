//! Bounded round-robin scans for calls awaiting coordinator discoveries.

use std::num::NonZeroUsize;

use kafka_driver_core::{CallFailure, CallId, CoordinatorKey, Delivery, Moment};

use crate::{RequestError, reactor::wait_queue::WaitQueue, request::ErasedRequest};

pub(in crate::reactor) struct CoordinatorWait {
    key: CoordinatorKey,
    request: Box<dyn ErasedRequest>,
}

impl CoordinatorWait {
    pub(in crate::reactor) fn new(key: CoordinatorKey, request: Box<dyn ErasedRequest>) -> Self {
        Self { key, request }
    }

    pub(super) const fn key(&self) -> &CoordinatorKey {
        &self.key
    }

    pub(super) fn call_id(&self) -> CallId {
        self.request.call_id()
    }
}

pub(super) struct CoordinatorWaiters {
    calls: WaitQueue<WaitingCoordinatorCall>,
    retained_bytes: usize,
    call_limit: NonZeroUsize,
    byte_limit: NonZeroUsize,
    scan_remaining: usize,
    due_pending: bool,
}

impl CoordinatorWaiters {
    pub(super) fn new(call_limit: NonZeroUsize, byte_limit: NonZeroUsize) -> Self {
        Self {
            calls: WaitQueue::new(call_limit),
            retained_bytes: 0,
            call_limit,
            byte_limit,
            scan_remaining: 0,
            due_pending: false,
        }
    }

    pub(super) fn admit(&mut self, waiting: CoordinatorWait, now: Moment) -> bool {
        let CoordinatorWait { key, mut request } = waiting;
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
        let bytes = waiting_bytes(&key, request.as_ref());
        let Some(total) = self.retained_bytes.checked_add(bytes) else {
            self.reject_capacity(request);
            return false;
        };
        if self.calls.len() == self.call_limit.get() || total > self.byte_limit.get() {
            self.reject_capacity(request);
            return false;
        }
        let waiting = WaitingCoordinatorCall {
            key,
            request,
            bytes,
        };
        if let Err(waiting) = self.calls.push(waiting, deadline) {
            self.reject_capacity(waiting.request);
            return false;
        }
        self.retained_bytes = total;
        true
    }

    pub(super) fn retract_last(&mut self, call_id: CallId) -> Option<Box<dyn ErasedRequest>> {
        if self.calls.back()?.request.call_id() != call_id {
            return None;
        }
        let (waiting, _) = self.calls.pop_back()?;
        self.retained_bytes -= waiting.bytes;
        Some(waiting.request)
    }

    pub(super) fn begin_scan(&mut self) {
        self.scan_remaining = self.calls.len();
    }

    pub(super) fn pop(&mut self, now: Moment) -> WaitingCoordinatorOutcome {
        if self.scan_remaining == 0 {
            return WaitingCoordinatorOutcome::Empty;
        }
        self.scan_remaining -= 1;
        let Some((waiting, deadline)) = self.calls.pop_front() else {
            self.scan_remaining = 0;
            return WaitingCoordinatorOutcome::Empty;
        };
        self.retained_bytes -= waiting.bytes;
        let Some(remaining) = deadline.duration_since(now) else {
            waiting.request.fail(deadline_exceeded());
            return WaitingCoordinatorOutcome::Settled;
        };
        if remaining.is_zero() {
            waiting.request.fail(deadline_exceeded());
            return WaitingCoordinatorOutcome::Settled;
        }
        WaitingCoordinatorOutcome::Ready { waiting, deadline }
    }

    pub(super) fn retain(&mut self, waiting: WaitingCoordinatorCall, deadline: Moment) -> bool {
        let bytes = waiting.bytes;
        if let Err(waiting) = self.calls.rotate_back(waiting, deadline) {
            self.reject_capacity(waiting.request);
            return false;
        }
        self.retained_bytes += bytes;
        true
    }

    pub(super) fn expire_due(&mut self, now: Moment, budget: usize) -> usize {
        if self.scan_remaining != 0 {
            return 0;
        }
        let mut settled = 0;
        while settled < budget {
            let Some((waiting, _)) = self.calls.take_due(now) else {
                break;
            };
            self.retained_bytes -= waiting.bytes;
            waiting.request.fail(deadline_exceeded());
            settled += 1;
        }
        self.refresh_due(now);
        settled
    }

    pub(super) fn refresh_due(&mut self, now: Moment) {
        self.due_pending = self
            .calls
            .next_deadline()
            .is_some_and(|deadline| deadline <= now);
    }

    pub(super) fn next_deadline(&self) -> Option<Moment> {
        self.calls.next_deadline()
    }

    pub(super) fn has_live_key(&self, key: &CoordinatorKey, now: Moment) -> bool {
        self.calls
            .iter()
            .any(|(waiting, deadline)| &waiting.key == key && deadline > now)
    }

    pub(super) const fn has_pending_scan(&self) -> bool {
        self.scan_remaining != 0 || self.due_pending
    }

    pub(super) fn fail_all(&mut self, failure: &RequestError) {
        for waiting in self.calls.drain() {
            waiting.request.fail(failure.clone());
        }
        self.retained_bytes = 0;
        self.scan_remaining = 0;
        self.due_pending = false;
    }

    fn reject_capacity(&self, request: Box<dyn ErasedRequest>) {
        request.fail(RequestError::RouteCapacityReached {
            call_limit: self.call_limit.get(),
            byte_limit: self.byte_limit.get(),
        });
    }
}

pub(super) fn waiting_bytes(key: &CoordinatorKey, request: &dyn ErasedRequest) -> usize {
    request.retained_bytes().saturating_add(key.heap_bytes())
}

pub(super) enum WaitingCoordinatorOutcome {
    Empty,
    Settled,
    Ready {
        waiting: WaitingCoordinatorCall,
        deadline: Moment,
    },
}

pub(super) struct WaitingCoordinatorCall {
    pub(super) key: CoordinatorKey,
    pub(super) request: Box<dyn ErasedRequest>,
    bytes: usize,
}

fn deadline_exceeded() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::DeadlineExceeded,
        delivery: Delivery::NotSent,
    }
}
