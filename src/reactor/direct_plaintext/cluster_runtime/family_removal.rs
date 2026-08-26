//! Atomic sparse removal for a broker's active physical lanes.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::BrokerId;

use crate::TrafficClass;

use super::{ClusterRuntime, family::FamilyLaneState, reclaimable};

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn remove_terminal_family(&mut self, broker_id: BrokerId) -> io::Result<bool> {
        let family = self
            .families
            .get(&broker_id)
            .ok_or_else(|| io::Error::other("Bornera broker family is stale"))?;
        let mut removals = Vec::with_capacity(TrafficClass::COUNT);
        for traffic in TrafficClass::ALL {
            if let FamilyLaneState::Active(owner, index) =
                self.family_lane_state(family, traffic)?
            {
                removals.push((index, owner));
            }
        }
        if removals
            .iter()
            .any(|(index, _)| !reclaimable(&self.lanes[*index]))
        {
            return Ok(false);
        }
        removals.sort_unstable_by(|left, right| right.0.cmp(&left.0));
        for (index, owner) in removals {
            self.remove_at(owner, index);
        }
        self.families.remove(&broker_id);
        Ok(true)
    }
}
