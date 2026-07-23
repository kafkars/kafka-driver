//! Bounded public invalidation barriers awaiting post-failure metadata evidence.

use std::{collections::VecDeque, num::NonZeroUsize};

use kafka_driver_core::{BrokerRoute, MetadataMachine, PartitionRoute};

use crate::{
    InvalidationDisposition, completion::CompletionSender, reactor::InvalidationSubscribers,
};

use super::{MetadataOwner, invalidation_target::InvalidationTarget};

pub(super) struct MetadataInvalidations {
    pending: VecDeque<PendingInvalidation>,
    capacity: usize,
    subscriber_count: usize,
    scan_remaining: usize,
}

impl MetadataInvalidations {
    pub(super) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            pending: VecDeque::with_capacity(capacity.get().min(16)),
            capacity: capacity.get(),
            subscriber_count: 0,
            scan_remaining: 0,
        }
    }

    pub(super) fn join_controller(
        &mut self,
        route: BrokerRoute,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> InvalidationJoin {
        let Some(index) = self
            .pending
            .iter()
            .position(|pending| pending.target.matches_controller(route))
        else {
            return InvalidationJoin::Missing(completion);
        };
        if !self.has_capacity() {
            return InvalidationJoin::Full(completion);
        }
        let pending = &mut self.pending[index];
        pending.subscribers.subscribe(completion);
        self.subscriber_count += 1;
        InvalidationJoin::Joined
    }

    pub(super) fn join_partition(
        &mut self,
        route: &PartitionRoute,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> InvalidationJoin {
        let Some(index) = self
            .pending
            .iter()
            .position(|pending| pending.target.matches_partition(route))
        else {
            return InvalidationJoin::Missing(completion);
        };
        if !self.has_capacity() {
            return InvalidationJoin::Full(completion);
        }
        let pending = &mut self.pending[index];
        pending.subscribers.subscribe(completion);
        self.subscriber_count += 1;
        InvalidationJoin::Joined
    }

    pub(super) fn has_capacity(&self) -> bool {
        self.subscriber_count < self.capacity
    }

    pub(super) fn push_controller(
        &mut self,
        route: BrokerRoute,
        completion: CompletionSender<InvalidationDisposition>,
    ) {
        self.pending.push_back(PendingInvalidation {
            target: InvalidationTarget::controller(route),
            subscribers: InvalidationSubscribers::new(completion),
        });
        self.subscriber_count += 1;
    }

    pub(super) fn push_partition(
        &mut self,
        route: PartitionRoute,
        completion: CompletionSender<InvalidationDisposition>,
    ) {
        self.pending.push_back(PendingInvalidation {
            target: InvalidationTarget::partition(route),
            subscribers: InvalidationSubscribers::new(completion),
        });
        self.subscriber_count += 1;
    }

    pub(super) fn begin_scan(&mut self) {
        self.scan_remaining = self.pending.len();
    }

    pub(super) fn scan(
        &mut self,
        machine: &MetadataMachine,
        budget: NonZeroUsize,
    ) -> InvalidationProgress {
        let examined = self.scan_remaining.min(budget.get());
        let mut settled = 0;
        for _ in 0..examined {
            let Some(pending) = self.pending.pop_front() else {
                self.scan_remaining = 0;
                break;
            };
            self.scan_remaining -= 1;
            if let Some(disposition) = pending.target.settled(machine) {
                let subscribers = pending.subscribers.len();
                pending.subscribers.complete(disposition);
                self.release(subscribers);
                settled += subscribers;
            } else {
                self.pending.push_back(pending);
            }
        }
        InvalidationProgress {
            examined,
            settled,
            more_work: self.scan_remaining != 0,
        }
    }

    pub(super) const fn has_pending_scan(&self) -> bool {
        self.scan_remaining != 0
    }

    pub(super) fn fail_all(&mut self) {
        for pending in self.pending.drain(..) {
            pending
                .subscribers
                .complete(InvalidationDisposition::Unavailable);
        }
        self.subscriber_count = 0;
        self.scan_remaining = 0;
    }

    fn release(&mut self, subscribers: usize) {
        debug_assert!(subscribers <= self.subscriber_count);
        self.subscriber_count -= subscribers;
    }
}

struct PendingInvalidation {
    target: InvalidationTarget,
    subscribers: InvalidationSubscribers,
}

pub(super) enum InvalidationJoin {
    Missing(CompletionSender<InvalidationDisposition>),
    Joined,
    Full(CompletionSender<InvalidationDisposition>),
}

pub(super) struct InvalidationProgress {
    examined: usize,
    settled: usize,
    more_work: bool,
}

impl InvalidationProgress {
    pub(super) const fn made_progress(&self) -> bool {
        self.examined != 0 || self.settled != 0
    }

    pub(super) const fn more_work(&self) -> bool {
        self.more_work
    }
}

impl MetadataOwner {
    pub(in crate::reactor) fn drain_invalidation_waiters(&mut self) -> bool {
        let progress = self
            .invalidations
            .scan(&self.machine, self.limits.partition_admission_budget());
        progress.made_progress() || progress.more_work()
    }
}
