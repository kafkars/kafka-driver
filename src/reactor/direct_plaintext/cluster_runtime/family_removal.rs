//! Atomic sparse removal for a broker's active physical lanes.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{BrokerId, Moment};

use crate::{TrafficClass, reactor::causality::CausalSequence};

use super::{ClusterRuntime, family::FamilyLaneState, reclaimable};

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn remove_terminal_family(&mut self, broker_id: BrokerId) -> io::Result<bool> {
        let mut removals = self.family_removals(broker_id)?;
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
        self.normalize_refresh_cursor();
        self.routes
            .retain(|lane, state| lane.broker_id() != broker_id || state.advertised.is_some());
        for state in self
            .routes
            .values_mut()
            .filter(|state| state.lane.broker_id() == broker_id)
        {
            state.clear_installed();
        }
        Ok(true)
    }

    pub(super) fn begin_family_retirement(
        &mut self,
        broker_id: BrokerId,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        let removals = self.family_removals(broker_id)?;
        let changed = self
            .families
            .get_mut(&broker_id)
            .ok_or_else(|| io::Error::other("Bornera broker family is stale"))?
            .begin_retirement();
        if !changed {
            return Ok(false);
        }
        for (index, _) in removals {
            if self.lanes[index].is_terminal() {
                continue;
            }
            self.connections
                .access(&mut self.lanes[index])
                .begin_session_drain(now, causality)?;
        }
        Ok(true)
    }

    pub(super) fn family_reclaimable(&self, broker_id: BrokerId) -> io::Result<bool> {
        Ok(self
            .family_removals(broker_id)?
            .iter()
            .all(|(index, _)| reclaimable(&self.lanes[*index])))
    }

    pub(super) fn family_active_count(&self, broker_id: BrokerId) -> io::Result<usize> {
        self.family_removals(broker_id).map(|lanes| lanes.len())
    }

    fn family_removals(
        &self,
        broker_id: BrokerId,
    ) -> io::Result<Vec<(usize, super::super::endpoint_refresh::DirectRefreshOwner)>> {
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
        Ok(removals)
    }
}
