//! Routed and settled outcomes from one bounded partition-wait turn.

use kafka_driver_core::PartitionRoute;

use crate::request::ErasedRequest;

#[derive(Default)]
pub(in crate::reactor) struct PartitionWaitProgress {
    pub(super) routed: Vec<RoutedPartitionCall>,
    pub(super) settled: usize,
    pub(super) examined: usize,
    pub(super) more_work: bool,
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
    pub(super) route: PartitionRoute,
    pub(super) request: Box<dyn ErasedRequest>,
}

impl RoutedPartitionCall {
    pub(in crate::reactor) const fn route(&self) -> &PartitionRoute {
        &self.route
    }

    pub(in crate::reactor) fn into_request(self) -> Box<dyn ErasedRequest> {
        self.request
    }
}
