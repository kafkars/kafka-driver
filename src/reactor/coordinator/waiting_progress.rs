//! Routed and settled outcomes from one bounded coordinator-wait turn.

use kafka_driver_core::CoordinatorRoute;

use crate::request::ErasedRequest;

#[derive(Default)]
pub(in crate::reactor) struct CoordinatorWaitProgress {
    pub(super) routed: Vec<RoutedCoordinatorCall>,
    pub(super) examined: usize,
    pub(super) settled: usize,
    pub(super) more_work: bool,
}

impl CoordinatorWaitProgress {
    pub(in crate::reactor) fn made_progress(&self) -> bool {
        self.examined != 0 || self.settled != 0 || !self.routed.is_empty()
    }

    pub(in crate::reactor) const fn more_work(&self) -> bool {
        self.more_work
    }

    pub(in crate::reactor) fn into_routed(self) -> Vec<RoutedCoordinatorCall> {
        self.routed
    }
}

pub(in crate::reactor) struct RoutedCoordinatorCall {
    pub(super) route: CoordinatorRoute,
    pub(super) request: Box<dyn ErasedRequest>,
}

impl RoutedCoordinatorCall {
    pub(in crate::reactor) const fn route(&self) -> &CoordinatorRoute {
        &self.route
    }

    pub(in crate::reactor) fn into_request(self) -> Box<dyn ErasedRequest> {
        self.request
    }
}
