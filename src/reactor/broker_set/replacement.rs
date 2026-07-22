//! Broker-slot retirement, reassignment, and deferred resolved-child installation.

use kafka_driver_core::{
    BrokerEndpoint, BrokerResolutionMachine, BrokerResolutionState, BrokerRoute, ConnectionEpoch,
    ResolvedAddressSet,
};

use crate::RequestError;

use super::{BrokerLane, child::BrokerChild};

pub(super) struct PendingBroker {
    pub(super) route: BrokerRoute,
    pub(super) epoch: ConnectionEpoch,
    pub(super) endpoint: BrokerEndpoint,
    pub(super) addresses: ResolvedAddressSet,
}

impl BrokerChild {
    pub(super) fn retain_route(&mut self, route: BrokerRoute, endpoint: &BrokerEndpoint) {
        self.retired = false;
        self.route = Some(route);
        let pending_is_stale = self
            .pending_install
            .as_ref()
            .is_some_and(|pending| pending.route != route);
        let resolution_is_stale = matches!(
            self.resolution.state(),
            BrokerResolutionState::Resolving { route: current, .. } if *current != route
        );
        let active_endpoint_changed = self
            .endpoint
            .as_ref()
            .is_some_and(|current| current != endpoint);
        if pending_is_stale {
            self.pending_install = None;
        }
        if pending_is_stale || resolution_is_stale || active_endpoint_changed {
            self.refresh_in_flight = false;
            self.waiting.fail_all(&RequestError::RouteUnavailable);
        }
    }

    pub(super) fn retire(&mut self) {
        if self.retired {
            return;
        }
        self.retired = true;
        self.retirement_started = false;
        self.pending_install = None;
        self.refresh_in_flight = false;
        self.waiting.fail_all(&RequestError::RouteUnavailable);
    }

    pub(super) fn is_reusable(&self) -> bool {
        self.retired
            && self.waiting.is_empty()
            && self.pending_install.is_none()
            && self
                .connection
                .as_ref()
                .is_none_or(super::super::broker::SingleBroker::is_terminal)
    }

    pub(super) fn reassign(&mut self, lane: BrokerLane) {
        self.lane = lane;
        self.resolution = BrokerResolutionMachine::new(lane.broker_id());
        self.route = None;
        self.endpoint = None;
        self.retired = false;
        self.retirement_started = false;
        self.refresh_in_flight = false;
    }

    pub(super) fn stage(&mut self, pending: PendingBroker) {
        self.pending_install = Some(pending);
    }

    pub(super) fn take_installable(&mut self) -> Option<PendingBroker> {
        let terminal = self
            .connection
            .as_ref()
            .is_none_or(super::super::broker::SingleBroker::is_terminal);
        terminal.then(|| self.pending_install.take()).flatten()
    }

    pub(super) fn abandon_pending(&mut self) {
        self.pending_install = None;
    }
}
