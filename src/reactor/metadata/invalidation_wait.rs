//! Bounded public invalidation barriers awaiting post-failure metadata evidence.

use std::{collections::VecDeque, num::NonZeroUsize};

use kafka_driver_core::{BrokerRoute, MetadataMachine, MetadataQuery, PartitionRoute};

use crate::{InvalidationDisposition, completion::CompletionSender};

use super::MetadataOwner;

pub(super) struct MetadataInvalidations {
    pending: VecDeque<PendingInvalidation>,
    capacity: usize,
    scan_remaining: usize,
}

impl MetadataInvalidations {
    pub(super) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            pending: VecDeque::with_capacity(capacity.get().min(16)),
            capacity: capacity.get(),
            scan_remaining: 0,
        }
    }

    pub(super) fn duplicate_controller(
        &self,
        route: BrokerRoute,
    ) -> Option<InvalidationDisposition> {
        self.pending
            .iter()
            .find_map(|pending| match &pending.target {
                InvalidationTarget::Controller { route: current } if *current == route => {
                    Some(InvalidationDisposition::Coalesced)
                }
                _ => None,
            })
    }

    pub(super) fn duplicate_partition(
        &self,
        route: &PartitionRoute,
    ) -> Option<InvalidationDisposition> {
        self.pending
            .iter()
            .find_map(|pending| match &pending.target {
                InvalidationTarget::Partition { route: current } if current.is_same_fact(route) => {
                    Some(InvalidationDisposition::Coalesced)
                }
                _ => None,
            })
    }

    pub(super) fn has_capacity(&self) -> bool {
        self.pending.len() < self.capacity
    }

    pub(super) fn push_controller(
        &mut self,
        route: BrokerRoute,
        completion: CompletionSender<InvalidationDisposition>,
    ) {
        self.pending.push_back(PendingInvalidation {
            target: InvalidationTarget::Controller { route },
            completion,
        });
    }

    pub(super) fn push_partition(
        &mut self,
        route: PartitionRoute,
        completion: CompletionSender<InvalidationDisposition>,
    ) {
        self.pending.push_back(PendingInvalidation {
            target: InvalidationTarget::Partition { route },
            completion,
        });
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
            if let Some(disposition) = settled_disposition(machine, &pending.target) {
                let _ = pending.completion.complete(disposition);
                settled += 1;
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
            let _ = pending
                .completion
                .complete(InvalidationDisposition::Unavailable);
        }
        self.scan_remaining = 0;
    }
}

fn settled_disposition(
    machine: &MetadataMachine,
    target: &InvalidationTarget,
) -> Option<InvalidationDisposition> {
    let (revoked, query) = match target {
        InvalidationTarget::Controller { route } => (
            machine.controller_revocation_pending(*route),
            MetadataQuery::Cluster,
        ),
        InvalidationTarget::Partition { route } => (
            machine.partition_revocation_pending(route),
            MetadataQuery::Topic(route.topic().clone()),
        ),
    };
    if !revoked {
        return Some(InvalidationDisposition::Applied);
    }
    (!machine.query_pending(&query)).then_some(InvalidationDisposition::Unavailable)
}

struct PendingInvalidation {
    target: InvalidationTarget,
    completion: CompletionSender<InvalidationDisposition>,
}

enum InvalidationTarget {
    Controller { route: BrokerRoute },
    Partition { route: PartitionRoute },
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
