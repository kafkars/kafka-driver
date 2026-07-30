//! Routed and settled outcomes from one bounded controller-wait turn.

use kafka_driver_core::BrokerRoute;

use crate::{api::RouteFact, request::ErasedRequest};

use super::controller_routing::ClusterRouteTarget;

#[derive(Default)]
pub(in crate::reactor) struct ControllerWaitProgress {
    pub(super) routed: Vec<RoutedControllerCall>,
    pub(super) settled: usize,
    pub(super) examined: usize,
    pub(super) more_work: bool,
}

impl ControllerWaitProgress {
    pub(in crate::reactor) fn into_routed(self) -> Vec<RoutedControllerCall> {
        self.routed
    }

    pub(in crate::reactor) fn made_progress(&self) -> bool {
        self.examined != 0 || self.settled != 0 || !self.routed.is_empty()
    }

    pub(in crate::reactor) const fn more_work(&self) -> bool {
        self.more_work
    }
}

pub(in crate::reactor) struct RoutedControllerCall {
    pub(super) route: BrokerRoute,
    pub(super) target: ClusterRouteTarget,
    pub(super) request: Box<dyn ErasedRequest>,
}

impl RoutedControllerCall {
    pub(in crate::reactor) const fn route(&self) -> BrokerRoute {
        self.route
    }

    pub(in crate::reactor) fn fact(&self) -> RouteFact {
        match self.target {
            ClusterRouteTarget::Controller => RouteFact::Controller(self.route),
            ClusterRouteTarget::Broker(_) => RouteFact::Broker(self.route),
        }
    }

    pub(in crate::reactor) fn into_request(self) -> Box<dyn ErasedRequest> {
        self.request
    }
}
