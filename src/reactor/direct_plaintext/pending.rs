//! Indexed deadline and FIFO ownership while the direct Kafka session is not ready.

use std::num::NonZeroUsize;

use kafka_driver_core::Moment;

use crate::{RequestError, reactor::wait_queue::WaitQueue, request::ErasedRequest};

use super::owner::deadline_exceeded;

pub(super) struct PendingRequests {
    requests: WaitQueue<PendingRequest>,
    retained_bytes: usize,
    count_limit: NonZeroUsize,
    byte_limit: NonZeroUsize,
}

impl PendingRequests {
    pub(super) fn new(count_limit: NonZeroUsize, byte_limit: NonZeroUsize) -> Self {
        Self {
            requests: WaitQueue::new(count_limit),
            retained_bytes: 0,
            count_limit,
            byte_limit,
        }
    }

    pub(super) fn push(&mut self, mut request: Box<dyn ErasedRequest>, now: Moment) {
        let deadline = match request.establish_deadline(now) {
            Ok(deadline) if deadline > now => deadline,
            Ok(_) => {
                request.fail(deadline_exceeded());
                return;
            }
            Err(failure) => {
                request.fail(failure);
                return;
            }
        };
        let bytes = request.retained_bytes();
        let next = self.retained_bytes.checked_add(bytes);
        if self.requests.len() == self.count_limit.get()
            || next.is_none_or(|retained| retained > self.byte_limit.get())
        {
            request.fail(self.capacity_failure());
            return;
        }
        let pending = PendingRequest { request, bytes };
        if let Err(pending) = self.requests.push(pending, deadline) {
            pending.request.fail(self.capacity_failure());
            return;
        }
        self.retained_bytes = next.unwrap_or(usize::MAX);
    }

    pub(super) fn pop(&mut self) -> Option<Box<dyn ErasedRequest>> {
        let (pending, _) = self.requests.pop_front()?;
        self.retained_bytes -= pending.bytes;
        Some(pending.request)
    }

    pub(super) fn expire_due(&mut self, now: Moment, budget: usize) -> PendingExpiration {
        let mut settled = 0;
        while settled < budget {
            let Some((pending, _)) = self.requests.take_due(now) else {
                break;
            };
            self.retained_bytes -= pending.bytes;
            pending.request.fail(deadline_exceeded());
            settled += 1;
        }
        let more_due = self
            .requests
            .next_deadline()
            .is_some_and(|deadline| deadline <= now);
        PendingExpiration { settled, more_due }
    }

    pub(super) fn fail_bounded(&mut self, failure: &RequestError, budget: usize) -> usize {
        let mut settled = 0;
        while settled < budget {
            let Some(request) = self.pop() else {
                break;
            };
            request.fail(failure.clone());
            settled += 1;
        }
        settled
    }

    pub(super) fn next_deadline(&self) -> Option<Moment> {
        self.requests.next_deadline()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    fn capacity_failure(&self) -> RequestError {
        RequestError::RouteCapacityReached {
            call_limit: self.count_limit.get(),
            byte_limit: self.byte_limit.get(),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct PendingExpiration {
    settled: usize,
    more_due: bool,
}

impl PendingExpiration {
    pub(super) const fn settled(self) -> usize {
        self.settled
    }

    pub(super) const fn more_due(self) -> bool {
        self.more_due
    }
}

struct PendingRequest {
    request: Box<dyn ErasedRequest>,
    bytes: usize,
}
