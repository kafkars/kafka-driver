//! Linear family-slot scheduling for discovered-broker endpoint refreshes.

use std::io;

use bornera::RegisteredTransport;

use crate::{TrafficClass, reactor::BrokerLane};

use super::super::route_turn::advance_cursor;
use super::{ClusterRuntime, fence::family_refresh_fence_valid};
use crate::reactor::direct_plaintext::endpoint_refresh::DirectRefreshOwner;

enum SlotProbe {
    Skip,
    Ready(DirectRefreshOwner),
    Missing(DirectRefreshOwner),
    LaneError(usize, &'static str),
    Error(&'static str),
}

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn next_broker_endpoint_refresh_owner(
        &mut self,
    ) -> io::Result<Option<DirectRefreshOwner>> {
        self.normalize_refresh_cursor();
        if self.refresh_turn.is_empty() {
            return Ok(None);
        }
        let start = self.refresh_cursor;
        for offset in 0..self.refresh_turn.len() {
            let cursor = advance_cursor(start, offset, self.refresh_turn.len());
            let lane = self.refresh_turn[cursor];
            let Some(owner) = self.refresh_family_slot(lane)? else {
                continue;
            };
            self.refresh_cursor = advance_cursor(cursor, 1, self.refresh_turn.len());
            return Ok(Some(owner));
        }
        Ok(None)
    }

    pub(super) fn prepare_endpoint_refresh_turn(&mut self) -> io::Result<()> {
        self.prepare_refresh_turn();
        self.validate_refresh_shape()
    }

    fn prepare_refresh_turn(&mut self) {
        self.refresh_turn.clear();
        self.refresh_turn
            .extend(self.families.keys().flat_map(|&broker_id| {
                TrafficClass::ALL
                    .into_iter()
                    .map(move |traffic| BrokerLane::new(broker_id, traffic))
            }));
    }

    fn validate_refresh_shape(&mut self) -> io::Result<()> {
        let mut expected = usize::from(self.seed.is_some());
        for index in 0..self.refresh_turn.len() {
            expected = expected
                .checked_add(self.validate_family_refresh_shape(self.refresh_turn[index])?)
                .ok_or_else(|| io::Error::other("Bornera refresh lane count overflowed"))?;
        }
        if expected == self.lanes.len() && expected == self.slots.len() {
            return Ok(());
        }
        Err(self.diagnose_refresh_shape("Bornera refresh family and physical lane counts diverged"))
    }

    fn validate_family_refresh_shape(&mut self, lane: BrokerLane) -> io::Result<usize> {
        let (owner, active) = {
            let family = self
                .families
                .get(&lane.broker_id())
                .ok_or_else(|| io::Error::other("Bornera refresh family slot vanished"))?;
            (
                super::super::refresh_owner(family.owner(lane.traffic_class())),
                family.is_active(lane.traffic_class()),
            )
        };
        if !active {
            if self.slots.contains_key(&owner) {
                return Err(self.diagnose_refresh_shape(
                    "Bornera dormant refresh family owns a physical slot",
                ));
            }
            return Ok(0);
        }
        let Some(index) = self.slots.get(&owner).copied() else {
            return Err(self.diagnose_refresh_shape("Bornera active refresh family lost its slot"));
        };
        let Some(physical) = self.lanes.get(index) else {
            return Err(
                self.diagnose_refresh_shape("Bornera active refresh slot is outside the lane set")
            );
        };
        if physical.refresh_owner() != owner {
            return Err(self.diagnose_refresh_shape("Bornera active refresh slot owner diverged"));
        }
        Ok(1)
    }

    fn diagnose_refresh_shape(&mut self, fallback: &'static str) -> io::Error {
        let mut failure = None;
        for index in 0..self.lanes.len() {
            let owner = self.lanes[index].refresh_owner();
            if self.seed.is_some_and(|seed| seed.owner == owner) {
                continue;
            }
            match self.family_refresh_lane(owner) {
                Ok(Some(_)) => {}
                Ok(None) => {
                    failure = Some((
                        index,
                        io::Error::other("Bornera endpoint-refresh lane lost its broker family"),
                    ));
                    break;
                }
                Err(error) => {
                    failure = Some((index, error));
                    break;
                }
            }
        }
        match failure {
            Some((index, error)) => self.fail_refresh_lane(index, error),
            None => io::Error::other(fallback),
        }
    }

    fn refresh_family_slot(&mut self, lane: BrokerLane) -> io::Result<Option<DirectRefreshOwner>> {
        let probe = self.probe_refresh_family_slot(lane);
        match probe {
            SlotProbe::Skip => Ok(None),
            SlotProbe::Ready(owner) => Ok(Some(owner)),
            SlotProbe::Missing(owner) => {
                let error = io::Error::other("Bornera refresh family lost its active slot");
                match self.checked_family_refresh_lane(owner) {
                    Err(fenced) => Err(fenced),
                    Ok(_) => Err(error),
                }
            }
            SlotProbe::LaneError(index, message) => {
                Err(self.fail_refresh_lane(index, io::Error::other(message)))
            }
            SlotProbe::Error(message) => Err(io::Error::other(message)),
        }
    }

    fn probe_refresh_family_slot(&self, lane: BrokerLane) -> SlotProbe {
        let Some(family) = self.families.get(&lane.broker_id()) else {
            return SlotProbe::Error("Bornera refresh family slot vanished");
        };
        if family.is_retiring() || !family.is_active(lane.traffic_class()) {
            return SlotProbe::Skip;
        }
        let owner = super::super::refresh_owner(family.owner(lane.traffic_class()));
        let Some(index) = self.slots.get(&owner).copied() else {
            return SlotProbe::Missing(owner);
        };
        let Some(physical) = self.lanes.get(index) else {
            return SlotProbe::Error("Bornera refresh family slot is outside the lane set");
        };
        if physical.refresh_owner() != owner {
            return SlotProbe::LaneError(index, "Bornera refresh family slot owner diverged");
        }
        if !self.family_endpoint_is_current(lane.broker_id(), family.endpoint())
            || physical.is_terminal()
        {
            return SlotProbe::Skip;
        }
        if !family_refresh_fence_valid(physical, owner, family.endpoint()) {
            return SlotProbe::LaneError(
                index,
                "Bornera broker endpoint-refresh lifecycle fence diverged",
            );
        }
        if physical.endpoint_refresh_needed() {
            SlotProbe::Ready(owner)
        } else {
            SlotProbe::Skip
        }
    }
}
