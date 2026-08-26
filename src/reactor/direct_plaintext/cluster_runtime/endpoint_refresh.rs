//! Seed-prioritized, cursor-fair endpoint refresh ownership for one cluster set.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{BrokerState, DnsOutcome, Moment};

use crate::reactor::causality::CausalSequence;

use super::{ClusterRuntime, seed_rotation::SeedBootstrapState};
use crate::reactor::direct_plaintext::endpoint_refresh::{
    DirectEndpointRefresh, DirectRefreshOwner,
};

#[path = "endpoint_refresh_fence.rs"]
mod fence;

#[path = "endpoint_refresh_schedule.rs"]
mod schedule;

#[cfg(test)]
#[path = "endpoint_refresh_test.rs"]
mod test;

#[cfg(test)]
#[path = "endpoint_refresh_edge_test.rs"]
mod edge_test;

#[cfg(test)]
#[path = "endpoint_refresh_invariant_test.rs"]
mod invariant_test;

#[cfg(test)]
#[path = "endpoint_refresh_support_test.rs"]
mod test_support;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClusterEndpointRefreshAction {
    SeedBootstrap,
    Broker(DirectRefreshOwner),
}

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn next_endpoint_refresh_action(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<Option<ClusterEndpointRefreshAction>> {
        let seed = self.seed_lane_index().map(drop);
        self.finish_host_result(seed)?;
        self.prepare_seed_bootstrap_restart(now, causality)?;
        if matches!(self.seed_bootstrap, SeedBootstrapState::RestartPending(_)) {
            return Ok(Some(ClusterEndpointRefreshAction::SeedBootstrap));
        }
        let shape = self.prepare_endpoint_refresh_turn();
        self.finish_host_result(shape)?;
        let result = self.next_broker_endpoint_refresh_owner();
        self.finish_host_result(result.map(|owner| owner.map(ClusterEndpointRefreshAction::Broker)))
    }

    pub(super) fn take_broker_endpoint_refresh(
        &mut self,
        owner: DirectRefreshOwner,
    ) -> io::Result<Option<DirectEndpointRefresh>> {
        let result = self.try_take_broker_endpoint_refresh(owner);
        self.finish_host_result(result)
    }

    pub(super) fn defer_broker_endpoint_refresh(
        &mut self,
        refresh: &DirectEndpointRefresh,
    ) -> io::Result<bool> {
        let result = self.try_defer_broker_endpoint_refresh(refresh);
        self.finish_host_result(result)
    }

    pub(super) fn complete_broker_endpoint_refresh(
        &mut self,
        owner: DirectRefreshOwner,
        outcome: DnsOutcome,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        let result = self.try_complete_broker_endpoint_refresh(owner, outcome, now, causality);
        self.finish_host_result(result)
    }

    fn try_take_broker_endpoint_refresh(
        &mut self,
        owner: DirectRefreshOwner,
    ) -> io::Result<Option<DirectEndpointRefresh>> {
        let Some(current) = self.checked_family_refresh_lane(owner)? else {
            return Ok(None);
        };
        if current.retiring || !current.current || self.lanes[current.index].is_terminal() {
            return Ok(None);
        }
        self.require_family_refresh_endpoint(&current)?;
        if !self.lanes[current.index].endpoint_refresh_needed() {
            return Ok(None);
        }
        match self.lanes[current.index].take_endpoint_refresh() {
            Ok(Some(refresh))
                if refresh.owner() == owner && refresh.endpoint() == &current.endpoint =>
            {
                Ok(Some(refresh))
            }
            Ok(Some(_)) => Err(self.fail_refresh_lane(
                current.index,
                io::Error::other("Bornera endpoint-refresh take fence diverged"),
            )),
            Ok(None) => Err(self.fail_refresh_lane(
                current.index,
                io::Error::other("Bornera endpoint-refresh vanished during take"),
            )),
            Err(error) => Err(self.fail_refresh_lane(current.index, error)),
        }
    }

    fn try_defer_broker_endpoint_refresh(
        &mut self,
        refresh: &DirectEndpointRefresh,
    ) -> io::Result<bool> {
        let Some(current) = self.checked_family_refresh_lane(refresh.owner())? else {
            return Ok(false);
        };
        if current.retiring || self.lanes[current.index].is_terminal() {
            return Ok(false);
        }
        self.require_family_refresh_endpoint(&current)?;
        if refresh.endpoint() != &current.endpoint {
            return Err(self.fail_refresh_lane(
                current.index,
                io::Error::other("Bornera broker endpoint-refresh family diverged"),
            ));
        }
        match self.lanes[current.index].defer_endpoint_refresh(refresh) {
            Ok(()) => Ok(true),
            Err(error) => Err(self.fail_refresh_lane(current.index, error)),
        }
    }

    fn try_complete_broker_endpoint_refresh(
        &mut self,
        owner: DirectRefreshOwner,
        outcome: DnsOutcome,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        let Some(current) = self.checked_family_refresh_lane(owner)? else {
            return Ok(false);
        };
        if current.retiring || self.lanes[current.index].is_terminal() {
            return Ok(false);
        }
        self.require_family_refresh_endpoint(&current)?;
        if !current.current {
            return self.restore_superseded_refresh(&current, &outcome);
        }
        self.connections
            .access(&mut self.lanes[current.index])
            .complete_endpoint_refresh_outcome(outcome, now, causality)
    }

    fn restore_superseded_refresh(
        &mut self,
        current: &fence::FamilyRefreshLane,
        outcome: &DnsOutcome,
    ) -> io::Result<bool> {
        let Some(refresh) = self.lanes[current.index].endpoint_refresh.clone() else {
            return Err(self.fail_refresh_lane(
                current.index,
                io::Error::other("Bornera endpoint-refresh supersession fence vanished"),
            ));
        };
        if outcome.epoch() != refresh.failed_epoch() {
            return Err(self.fail_refresh_lane(
                current.index,
                io::Error::other("Bornera endpoint-refresh outcome epoch diverged"),
            ));
        }
        let resolving = matches!(
            self.lanes[current.index].lifecycle.state(),
            BrokerState::Refreshing { failed_epoch, refresh: kafka_driver_core::AddressRefreshState::Resolving { .. }, .. }
                if failed_epoch == refresh.failed_epoch()
        );
        if !resolving {
            return Err(self.fail_refresh_lane(
                current.index,
                io::Error::other("Bornera endpoint-refresh supersession state diverged"),
            ));
        }
        match self.lanes[current.index].defer_endpoint_refresh(&refresh) {
            Ok(()) => Ok(false),
            Err(error) => Err(self.fail_refresh_lane(current.index, error)),
        }
    }
}
