//! Metadata-generation installation and discovered-route retention policy.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{BrokerDirectory, BrokerEndpoint, BrokerRoute};

use crate::{RequestError, reactor::BrokerLane};

use super::{ClusterRuntime, route_state::BrokerRouteState};

#[cfg(test)]
#[path = "route_directory_test.rs"]
mod test;

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn install_directory(&mut self, directory: &BrokerDirectory) -> io::Result<bool> {
        let result = self.try_install_directory(directory);
        self.finish_host_result(result)
    }

    fn try_install_directory(&mut self, directory: &BrokerDirectory) -> io::Result<bool> {
        let limit = self
            .driver
            .metadata()
            .broker_directory()
            .max_brokers()
            .get();
        if directory.len() > limit {
            return Err(io::Error::other(
                "Bornera broker directory capacity exceeded",
            ));
        }
        if let Some(current) = &self.directory {
            if current == directory {
                return Ok(false);
            }
            if current.generation() == directory.generation() {
                return Err(io::Error::other(
                    "Bornera broker directory generation diverged",
                ));
            }
            if directory.generation() < current.generation() {
                return Ok(false);
            }
        }
        for state in self.routes.values_mut() {
            let Some((route, endpoint)) = advertised(directory, state.lane) else {
                state.retire();
                continue;
            };
            state.retain_route(route, &endpoint);
        }
        self.routes.retain(|lane, state| {
            state.advertised.is_some() || self.families.contains_key(&lane.broker_id())
        });
        self.directory = Some(directory.clone());
        Ok(true)
    }

    pub(super) fn resolve_route(&self, route: BrokerRoute) -> Option<BrokerEndpoint> {
        self.directory
            .as_ref()?
            .resolve(route)
            .ok()
            .map(|entry| entry.endpoint().clone())
    }

    pub(super) fn route_is_current(&self, route: BrokerRoute, endpoint: &BrokerEndpoint) -> bool {
        self.resolve_route(route).as_ref() == Some(endpoint)
    }

    pub(super) fn insert_route_state(
        &mut self,
        lane: BrokerLane,
        route: BrokerRoute,
        endpoint: BrokerEndpoint,
    ) -> bool {
        if self.routes.contains_key(&lane) {
            return true;
        }
        let capacity = self
            .driver
            .metadata()
            .broker_directory()
            .max_brokers()
            .get()
            .saturating_mul(crate::TrafficClass::COUNT);
        if self.routes.len() == capacity {
            return false;
        }
        self.routes.insert(
            lane,
            BrokerRouteState::new(lane, route, endpoint, &self.driver),
        );
        true
    }

    pub(super) fn fail_stale_route(request: Box<dyn crate::request::ErasedRequest>) {
        request.fail(RequestError::RouteUnavailable);
    }
}

fn advertised(
    directory: &BrokerDirectory,
    lane: BrokerLane,
) -> Option<(BrokerRoute, BrokerEndpoint)> {
    let route = directory.route_to(lane.broker_id())?;
    let endpoint = directory.resolve(route).ok()?.endpoint().clone();
    Some((route, endpoint))
}
