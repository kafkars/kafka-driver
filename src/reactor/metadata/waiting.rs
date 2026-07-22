//! Bounded ownership for calls awaiting one exact topic-partition leader fact.

use std::{collections::VecDeque, num::NonZeroUsize};

use kafka_driver_core::{
    CallFailure, CallId, Delivery, MetadataMachine, MetadataQuery, Moment, PartitionId,
    PartitionRoute, TopicName,
};

use crate::{RequestError, request::ErasedRequest};

pub(super) struct PartitionWaiters {
    calls: VecDeque<WaitingPartitionCall>,
    retained_bytes: usize,
    call_limit: NonZeroUsize,
    byte_limit: NonZeroUsize,
    scan_remaining: usize,
}

impl PartitionWaiters {
    pub(super) fn new(call_limit: NonZeroUsize, byte_limit: NonZeroUsize) -> Self {
        Self {
            calls: VecDeque::with_capacity(call_limit.get().min(16)),
            retained_bytes: 0,
            call_limit,
            byte_limit,
            scan_remaining: 0,
        }
    }

    pub(super) fn admit(
        &mut self,
        topic: TopicName,
        partition: PartitionId,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> bool {
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
        self.calls.push_back(WaitingPartitionCall {
            query: MetadataQuery::Topic(topic),
            partition,
            request,
            deadline,
            bytes,
        });
        self.retained_bytes = retained_bytes;
        true
    }

    pub(super) fn retract_last(&mut self, call_id: CallId) -> Option<Box<dyn ErasedRequest>> {
        if self.calls.back()?.request.call_id() != call_id {
            return None;
        }
        let waiting = self.calls.pop_back()?;
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
        let examined = self.scan_remaining.min(budget.get());
        progress.examined = examined;
        for _ in 0..examined {
            let Some(mut waiting) = self.calls.pop_front() else {
                self.scan_remaining = 0;
                break;
            };
            self.scan_remaining -= 1;
            self.retained_bytes -= waiting.bytes;
            let Some(remaining) = waiting.deadline.duration_since(now) else {
                waiting.request.fail(deadline_exceeded());
                progress.settled += 1;
                continue;
            };
            if remaining.is_zero() {
                waiting.request.fail(deadline_exceeded());
                progress.settled += 1;
                continue;
            }
            let topic = topic(&waiting.query);
            if let Some(route) = machine
                .current()
                .and_then(|snapshot| snapshot.partition_route(topic, waiting.partition))
            {
                waiting.request.set_timeout(remaining);
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
            self.retained_bytes += waiting.bytes;
            self.calls.push_back(waiting);
        }
        progress.more_work = self.scan_remaining != 0;
        progress
    }

    pub(super) fn prepare_due_scan(&mut self, now: Moment) {
        if self.next_deadline().is_some_and(|deadline| deadline <= now) {
            self.begin_scan();
        }
    }

    pub(super) fn next_deadline(&self) -> Option<Moment> {
        self.calls.iter().map(|waiting| waiting.deadline).min()
    }

    pub(super) const fn has_pending_scan(&self) -> bool {
        self.scan_remaining != 0
    }

    pub(super) fn fail_all(&mut self, failure: &RequestError) {
        for waiting in self.calls.drain(..) {
            waiting.request.fail(failure.clone());
        }
        self.retained_bytes = 0;
        self.scan_remaining = 0;
    }

    fn reject_capacity(&self, request: Box<dyn ErasedRequest>) {
        request.fail(RequestError::RouteCapacityReached {
            call_limit: self.call_limit.get(),
            byte_limit: self.byte_limit.get(),
        });
    }
}

#[derive(Default)]
pub(in crate::reactor) struct PartitionWaitProgress {
    routed: Vec<RoutedPartitionCall>,
    settled: usize,
    examined: usize,
    more_work: bool,
}

impl PartitionWaitProgress {
    pub(in crate::reactor) fn into_routed(self) -> Vec<RoutedPartitionCall> {
        self.routed
    }

    pub(in crate::reactor) fn made_progress(&self) -> bool {
        self.examined != 0 || self.settled != 0 || !self.routed.is_empty()
    }

    pub(in crate::reactor) const fn more_work(&self) -> bool {
        self.more_work
    }
}

pub(in crate::reactor) struct RoutedPartitionCall {
    route: PartitionRoute,
    request: Box<dyn ErasedRequest>,
}

impl RoutedPartitionCall {
    pub(in crate::reactor) const fn route(&self) -> &PartitionRoute {
        &self.route
    }

    pub(in crate::reactor) fn into_request(self) -> Box<dyn ErasedRequest> {
        self.request
    }
}

struct WaitingPartitionCall {
    query: MetadataQuery,
    partition: PartitionId,
    request: Box<dyn ErasedRequest>,
    deadline: Moment,
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
