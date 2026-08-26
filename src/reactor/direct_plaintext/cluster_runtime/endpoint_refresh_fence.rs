//! Exact discovered-family membership and endpoint fences for refresh routing.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{BrokerEndpoint, BrokerId, BrokerState};

use crate::TrafficClass;

use super::super::{ClusterRuntime, family::FamilyLaneState, refresh_owner};
use crate::reactor::direct_plaintext::{endpoint_refresh::DirectRefreshOwner, owner::DirectLane};

pub(super) struct FamilyRefreshLane {
    pub(super) index: usize,
    pub(super) owner: DirectRefreshOwner,
    pub(super) endpoint: BrokerEndpoint,
    pub(super) current: bool,
    pub(super) retiring: bool,
}

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(in crate::reactor::direct_plaintext::cluster_runtime) fn normalize_refresh_cursor(
        &mut self,
    ) {
        let slots = self.families.len().saturating_mul(TrafficClass::COUNT);
        self.refresh_cursor = self.refresh_cursor.checked_rem(slots).unwrap_or(0);
    }

    pub(super) fn family_refresh_lane(
        &self,
        owner: DirectRefreshOwner,
    ) -> io::Result<Option<FamilyRefreshLane>> {
        let Some(index) = self.slots.get(&owner).copied() else {
            return Ok(None);
        };
        if self.lanes.get(index).map(DirectLane::refresh_owner) != Some(owner) {
            return Err(io::Error::other("Bornera endpoint-refresh slot diverged"));
        }
        let Some(broker_id) = self.unique_refresh_family(owner)? else {
            if self.seed.is_some_and(|seed| seed.owner == owner) {
                return Ok(None);
            }
            return Err(io::Error::other(
                "Bornera endpoint-refresh lane lost its broker family",
            ));
        };
        let family = &self.families[&broker_id];
        let traffic = TrafficClass::ALL
            .into_iter()
            .find(|traffic| refresh_owner(family.owner(*traffic)) == owner)
            .ok_or_else(|| io::Error::other("Bornera endpoint-refresh lane is unreserved"))?;
        match self.family_lane_state(family, traffic)? {
            FamilyLaneState::Active(current, active) if current == owner && active == index => {
                Ok(Some(FamilyRefreshLane {
                    index,
                    owner,
                    endpoint: family.endpoint().clone(),
                    current: self.family_endpoint_is_current(broker_id, family.endpoint()),
                    retiring: family.is_retiring(),
                }))
            }
            FamilyLaneState::Active(_, _) | FamilyLaneState::Dormant => Err(io::Error::other(
                "Bornera endpoint-refresh active family slot diverged",
            )),
        }
    }

    pub(super) fn checked_family_refresh_lane(
        &mut self,
        owner: DirectRefreshOwner,
    ) -> io::Result<Option<FamilyRefreshLane>> {
        let physical = self
            .lanes
            .iter()
            .position(|lane| lane.refresh_owner() == owner);
        match self.family_refresh_lane(owner) {
            Ok(None) if self.seed.is_some_and(|seed| seed.owner == owner) => Ok(None),
            Ok(None) => match physical {
                Some(index) => Err(self.fail_refresh_lane(
                    index,
                    io::Error::other("Bornera endpoint-refresh lane lost its cluster slot"),
                )),
                None => Ok(None),
            },
            Ok(current) => Ok(current),
            Err(error) => match physical {
                Some(index) => Err(self.fail_refresh_lane(index, error)),
                None => Err(error),
            },
        }
    }

    pub(super) fn require_family_refresh_endpoint(
        &mut self,
        current: &FamilyRefreshLane,
    ) -> io::Result<()> {
        if family_refresh_fence_valid(&self.lanes[current.index], current.owner, &current.endpoint)
        {
            return Ok(());
        }
        Err(self.fail_refresh_lane(
            current.index,
            io::Error::other("Bornera broker endpoint-refresh lifecycle fence diverged"),
        ))
    }

    pub(super) fn fail_refresh_lane(&mut self, index: usize, error: io::Error) -> io::Error {
        self.connections
            .access(&mut self.lanes[index])
            .host_fatal(error)
    }

    fn unique_refresh_family(&self, owner: DirectRefreshOwner) -> io::Result<Option<BrokerId>> {
        let mut matched = None;
        for (&broker_id, family) in &self.families {
            if family.contains(owner) && matched.replace(broker_id).is_some() {
                return Err(io::Error::other(
                    "Bornera endpoint-refresh owner spans broker families",
                ));
            }
        }
        Ok(matched)
    }

    pub(super) fn family_endpoint_is_current(
        &self,
        broker_id: BrokerId,
        endpoint: &BrokerEndpoint,
    ) -> bool {
        let Some(directory) = self.directory.as_ref() else {
            return false;
        };
        let Some(route) = directory.route_to(broker_id) else {
            return false;
        };
        directory
            .resolve(route)
            .is_ok_and(|entry| entry.endpoint() == endpoint)
    }
}

pub(super) fn family_refresh_fence_valid<T: RegisteredTransport>(
    lane: &DirectLane<T>,
    owner: DirectRefreshOwner,
    endpoint: &BrokerEndpoint,
) -> bool {
    match (&lane.endpoint_refresh, lane.lifecycle.state()) {
        (None, BrokerState::Refreshing { .. }) => false,
        (None, _) => true,
        (Some(refresh), BrokerState::Refreshing { failed_epoch, .. }) => {
            refresh.owner() == owner
                && refresh.endpoint() == endpoint
                && refresh.failed_epoch() == failed_epoch
        }
        (Some(_), _) => false,
    }
}
