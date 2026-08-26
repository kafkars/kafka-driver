//! Exact, side-effect-free RPC lending from semantic cluster ownership.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::BrokerRoute;

use crate::{
    TrafficClass,
    reactor::{BrokerLane, BrokerRpc, causality::CausalSequence},
};

use super::{ClusterRuntime, family::FamilyLaneState};
use crate::reactor::direct_plaintext::DirectBrokerRpc;

#[cfg(test)]
#[path = "rpc_access_test.rs"]
mod test;

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn seed_rpc<'lane, 'cause>(
        &'lane mut self,
        causality: &'cause mut CausalSequence,
    ) -> io::Result<Option<DirectBrokerRpc<'lane, 'cause, T>>> {
        let index = match self.seed_lane_index() {
            Ok(index) => index,
            Err(error) => return self.finish_host_result(Err(error)),
        };
        let Some(index) = index else {
            return Ok(None);
        };
        Ok(Some(DirectBrokerRpc::new(
            self.connections.access(&mut self.lanes[index]),
            causality,
        )))
    }

    pub(super) fn route_rpc<'lane, 'cause>(
        &'lane mut self,
        route: BrokerRoute,
        traffic: TrafficClass,
        causality: &'cause mut CausalSequence,
    ) -> io::Result<Option<DirectBrokerRpc<'lane, 'cause, T>>> {
        let index = match self.route_rpc_index(route, traffic) {
            Ok(index) => index,
            Err(error) => return self.finish_host_result(Err(error)),
        };
        let Some(index) = index else {
            return Ok(None);
        };
        Ok(Some(DirectBrokerRpc::new(
            self.connections.access(&mut self.lanes[index]),
            causality,
        )))
    }

    fn route_rpc_index(
        &self,
        route: BrokerRoute,
        traffic: TrafficClass,
    ) -> io::Result<Option<usize>> {
        let Some(endpoint) = self.resolve_route(route) else {
            return Ok(None);
        };
        let lane = BrokerLane::new(route.broker_id(), traffic);
        let Some(installed) = self
            .routes
            .get(&lane)
            .and_then(|state| state.installed.as_ref())
        else {
            return Ok(None);
        };
        if installed.route != route || installed.endpoint != endpoint {
            return Ok(None);
        }
        let Some(family) = self.families.get(&route.broker_id()) else {
            return Err(io::Error::other(
                "Bornera installed route has no broker family",
            ));
        };
        if family.is_retiring() {
            return Ok(None);
        }
        if family.endpoint() != &endpoint {
            return Err(io::Error::other(
                "Bornera installed route family endpoint diverged",
            ));
        }
        match self.family_lane_state(family, traffic)? {
            FamilyLaneState::Active(owner, index) if owner == installed.owner => Ok(Some(index)),
            FamilyLaneState::Active(_, _) => {
                Err(io::Error::other("Bornera installed route owner diverged"))
            }
            FamilyLaneState::Dormant => {
                Err(io::Error::other("Bornera installed route lane is dormant"))
            }
        }
    }

    pub(super) fn with_seed_rpc<R, E>(
        &mut self,
        causality: &mut CausalSequence,
        use_rpc: impl FnOnce(Option<&mut dyn BrokerRpc>) -> Result<R, E>,
    ) -> Result<R, ClusterRpcAccessError<E>> {
        let result = {
            let mut rpc = self
                .seed_rpc(causality)
                .map_err(ClusterRpcAccessError::Runtime)?;
            use_rpc(rpc.as_mut().map(|rpc| rpc as &mut dyn BrokerRpc))
                .map_err(ClusterRpcAccessError::Owner)
        };
        if result.is_err() {
            self.totalize_after_host_failure();
        }
        result
    }

    pub(super) fn with_route_rpc<R, E>(
        &mut self,
        route: BrokerRoute,
        traffic: TrafficClass,
        causality: &mut CausalSequence,
        use_rpc: impl FnOnce(Option<&mut dyn BrokerRpc>) -> Result<R, E>,
    ) -> Result<R, ClusterRpcAccessError<E>> {
        let result = {
            let mut rpc = self
                .route_rpc(route, traffic, causality)
                .map_err(ClusterRpcAccessError::Runtime)?;
            use_rpc(rpc.as_mut().map(|rpc| rpc as &mut dyn BrokerRpc))
                .map_err(ClusterRpcAccessError::Owner)
        };
        if result.is_err() {
            self.totalize_after_host_failure();
        }
        result
    }
}

#[derive(Debug)]
pub(in crate::reactor::direct_plaintext) enum ClusterRpcAccessError<E> {
    Runtime(io::Error),
    Owner(E),
}
