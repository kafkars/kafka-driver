//! Exact active and dormant ownership validation for one broker family.

use std::io;

use bornera::RegisteredTransport;

use crate::TrafficClass;

use super::{
    ClusterRuntime,
    family::{BrokerFamily, FamilyLaneState},
    refresh_owner,
};

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn family_lane_state(
        &self,
        family: &BrokerFamily,
        traffic: TrafficClass,
    ) -> io::Result<FamilyLaneState> {
        let owner = refresh_owner(family.owner(traffic));
        let slot = self.slots.get(&owner).copied();
        let mut physical = self
            .lanes
            .iter()
            .enumerate()
            .filter(|(_, lane)| lane.refresh_owner() == owner);
        let first = physical.next().map(|(index, _)| index);
        let unique = physical.next().is_none();
        match (family.is_active(traffic), slot, first, unique) {
            (true, Some(index), Some(actual), true) if index == actual => {
                Ok(FamilyLaneState::Active(owner, index))
            }
            (false, None, None, true) => Ok(FamilyLaneState::Dormant),
            _ => Err(io::Error::other(
                "Bornera broker family lane state diverged",
            )),
        }
    }
}
