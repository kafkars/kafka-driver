//! Bounded round-robin scans for calls awaiting exact partition-leader facts.

use std::num::NonZeroUsize;

use kafka_driver_core::{
    CallFailure, CallId, Delivery, MetadataMachine, MetadataQuery, Moment, PartitionId, TopicName,
};

use crate::{RequestError, reactor::wait_queue::WaitQueue, request::ErasedRequest};

use super::waiting_progress::{PartitionWaitProgress, RoutedPartitionCall};

pub(super) struct PartitionWaiters {
    calls: WaitQueue<WaitingPartitionCall>,
    retained_bytes: usize,
    call_limit: NonZeroUsize,
    byte_limit: NonZeroUsize,
    scan_remaining: usize,
    due_pending: bool,
}

impl PartitionWaiters {
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

    pub(super) fn admit(
        &mut self,
        topic: TopicName,
        partition: PartitionId,
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
        let bytes = waiting_bytes(&topic, request.as_ref());
        let Some(retained_bytes) = self.retained_bytes.checked_add(bytes) else {
            self.reject_capacity(request);
            return false;
        };
        if self.calls.len() == self.call_limit.get() || retained_bytes > self.byte_limit.get() {
            self.reject_capacity(request);
            return false;
        }
        let waiting = WaitingPartitionCall {
            query: MetadataQuery::Topic(topic),
            partition,
            request,
            bytes,
        };
        if let Err(waiting) = self.calls.push(waiting, deadline) {
            self.reject_capacity(waiting.request);
            return false;
        }
        self.retained_bytes = retained_bytes;
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

    pub(super) fn scan(
        &mut self,
        machine: &MetadataMachine,
        now: Moment,
        budget: NonZeroUsize,
    ) -> PartitionWaitProgress {
        let mut progress = PartitionWaitProgress::default();
        let mut remaining = budget.get();
        if self.scan_remaining == 0 {
            while remaining != 0 {
                let Some((waiting, _)) = self.calls.take_due(now) else {
                    break;
                };
                self.retained_bytes -= waiting.bytes;
                waiting.request.fail(deadline_exceeded());
                progress.examined += 1;
                progress.settled += 1;
                remaining -= 1;
            }
        }
        let examined = self.scan_remaining.min(remaining);
        progress.examined += examined;
        for _ in 0..examined {
            let Some((waiting, deadline)) = self.calls.pop_front() else {
                self.scan_remaining = 0;
                break;
            };
            self.scan_remaining -= 1;
            self.retained_bytes -= waiting.bytes;
            let Some(deadline_remaining) = deadline.duration_since(now) else {
                waiting.request.fail(deadline_exceeded());
                progress.settled += 1;
                continue;
            };
            if deadline_remaining.is_zero() {
                waiting.request.fail(deadline_exceeded());
                progress.settled += 1;
                continue;
            }
            let topic = topic(&waiting.query);
            if let Some(route) = machine
                .current()
                .and_then(|snapshot| snapshot.partition_route(topic, waiting.partition))
            {
                progress.routed.push(RoutedPartitionCall {
                    route,
                    request: waiting.request,
                });
                continue;
            }
            if !machine.query_pending(&waiting.query) {
                waiting.request.fail(RequestError::RouteUnavailable);
                progress.settled += 1;
                continue;
            }
            let bytes = waiting.bytes;
            if let Err(waiting) = self.calls.rotate_back(waiting, deadline) {
                self.reject_capacity(waiting.request);
                progress.settled += 1;
                continue;
            }
            self.retained_bytes += bytes;
        }
        self.due_pending = self
            .calls
            .next_deadline()
            .is_some_and(|deadline| deadline <= now);
        progress.more_work = self.scan_remaining != 0 || self.due_pending;
        progress
    }

    pub(super) fn next_deadline(&self) -> Option<Moment> {
        self.calls.next_deadline()
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

pub(super) fn waiting_bytes(topic: &TopicName, request: &dyn ErasedRequest) -> usize {
    request.retained_bytes().saturating_add(topic.heap_bytes())
}

struct WaitingPartitionCall {
    query: MetadataQuery,
    partition: PartitionId,
    request: Box<dyn ErasedRequest>,
    bytes: usize,
}

fn topic(query: &MetadataQuery) -> &TopicName {
    let MetadataQuery::Topic(topic) = query else {
        unreachable!("partition waiters store only exact topic queries");
    };
    topic
}

fn deadline_exceeded() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::DeadlineExceeded,
        delivery: Delivery::NotSent,
    }
}
