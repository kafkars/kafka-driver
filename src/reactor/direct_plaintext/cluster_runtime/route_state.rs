//! Per-traffic semantic route state outside replaceable physical Bornera lanes.

use kafka_driver_core::{
    BrokerEndpoint, BrokerResolutionMachine, BrokerResolutionState, BrokerRoute,
    ConnectionEpoch as DnsConnectionEpoch, DnsFailure, OutcomeStamp, ResolvedAddressSet,
};

use crate::{
    DriverLimits, RequestError,
    reactor::{BrokerLane, direct_plaintext::endpoint_refresh::DirectRefreshOwner},
};

use crate::reactor::route_waiting::RouteWaiting;

#[cfg(test)]
#[path = "route_state_test.rs"]
mod test;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AdvertisedRoute {
    pub(super) route: BrokerRoute,
    pub(super) endpoint: BrokerEndpoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct InstalledRoute {
    pub(super) route: BrokerRoute,
    pub(super) endpoint: BrokerEndpoint,
    pub(super) owner: DirectRefreshOwner,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PendingInstall {
    pub(super) route: BrokerRoute,
    pub(super) dns_epoch: DnsConnectionEpoch,
    pub(super) endpoint: BrokerEndpoint,
    pub(super) addresses: ResolvedAddressSet,
}

pub(super) struct BrokerRouteState {
    pub(super) lane: BrokerLane,
    pub(super) advertised: Option<AdvertisedRoute>,
    pub(super) installed: Option<InstalledRoute>,
    pub(super) resolution: BrokerResolutionMachine,
    pub(super) next_dns_epoch: Option<u64>,
    pub(super) pending_install: Option<PendingInstall>,
    pub(super) waiting: RouteWaiting,
    pub(super) last_dns_failure: Option<DnsFailure>,
    pub(super) route_failure_at: Option<OutcomeStamp>,
}

impl BrokerRouteState {
    pub(super) fn new(
        lane: BrokerLane,
        route: BrokerRoute,
        endpoint: BrokerEndpoint,
        driver: &DriverLimits,
    ) -> Self {
        let metadata = driver.metadata();
        Self {
            lane,
            advertised: Some(AdvertisedRoute { route, endpoint }),
            installed: None,
            resolution: BrokerResolutionMachine::new(lane.broker_id()),
            next_dns_epoch: Some(1),
            pending_install: None,
            waiting: RouteWaiting::new(
                metadata.waiting_calls(),
                metadata.waiting_bytes(),
                metadata.admission_budget(),
            ),
            last_dns_failure: None,
            route_failure_at: None,
        }
    }

    pub(super) fn retain_route(&mut self, route: BrokerRoute, endpoint: &BrokerEndpoint) {
        let previous = self.advertised.replace(AdvertisedRoute {
            route,
            endpoint: endpoint.clone(),
        });
        let route_changed = previous
            .as_ref()
            .is_some_and(|current| current.route != route);
        let endpoint_changed = previous
            .as_ref()
            .is_some_and(|current| &current.endpoint != endpoint);
        if route_changed {
            self.route_failure_at = None;
            self.last_dns_failure = None;
        }
        if endpoint_changed {
            self.waiting.fail_all(&RequestError::RouteUnavailable, None);
        }
        if let Some(installed) = self.installed.as_mut()
            && &installed.endpoint == endpoint
        {
            installed.route = route;
        }
        if self
            .pending_install
            .as_ref()
            .is_some_and(|pending| pending.route != route || &pending.endpoint != endpoint)
        {
            self.pending_install = None;
        }
    }

    pub(super) fn retire(&mut self) {
        self.advertised = None;
        self.pending_install = None;
        self.last_dns_failure = None;
        self.route_failure_at = None;
        self.waiting.fail_all(&RequestError::RouteUnavailable, None);
    }

    pub(super) fn advertises(&self, route: BrokerRoute, endpoint: &BrokerEndpoint) -> bool {
        self.advertised
            .as_ref()
            .is_some_and(|current| current.route == route && &current.endpoint == endpoint)
    }

    pub(super) fn is_resolving(&self, route: BrokerRoute, endpoint: &BrokerEndpoint) -> bool {
        matches!(
            self.resolution.state(),
            BrokerResolutionState::Resolving {
                route: current,
                endpoint: current_endpoint,
                ..
            } if *current == route && current_endpoint == endpoint
        )
    }

    pub(super) fn pending_is_current(&self, route: BrokerRoute, endpoint: &BrokerEndpoint) -> bool {
        self.pending_install
            .as_ref()
            .is_some_and(|pending| pending.route == route && &pending.endpoint == endpoint)
    }

    pub(super) fn needs_resolution(
        &self,
        route: BrokerRoute,
        endpoint: &BrokerEndpoint,
        physical_is_current: bool,
    ) -> bool {
        !physical_is_current
            && !self.is_resolving(route, endpoint)
            && !self.pending_is_current(route, endpoint)
    }

    pub(super) fn reserve_dns_epoch(&mut self) -> std::io::Result<DnsConnectionEpoch> {
        let raw = self
            .next_dns_epoch
            .ok_or_else(|| std::io::Error::other("broker DNS epoch exhausted"))?;
        self.next_dns_epoch = raw.checked_add(1);
        Ok(DnsConnectionEpoch::from_raw(raw))
    }

    pub(super) fn mark_installed(
        &mut self,
        route: BrokerRoute,
        endpoint: BrokerEndpoint,
        owner: DirectRefreshOwner,
    ) {
        self.installed = Some(InstalledRoute {
            route,
            endpoint,
            owner,
        });
        self.pending_install = None;
        self.last_dns_failure = None;
    }
}
