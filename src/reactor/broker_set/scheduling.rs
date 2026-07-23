//! Exact runnable-index synchronization after one broker-lane mutation.

use super::{BrokerLane, BrokerSet, BrokerSetError};

impl BrokerSet {
    pub(super) fn sync_lane(&mut self, lane: BrokerLane) -> Result<(), BrokerSetError> {
        let Some(child) = self
            .child_index(lane)
            .and_then(|index| self.children.get(index))
        else {
            self.remove_lane_indexes(lane);
            return Ok(());
        };
        let needs_refresh = child.needs_address_refresh();
        let needs_turn = child.needs_runnable_turn();
        let is_reusable = child.is_reusable();
        let deadline = child.next_deadline();
        self.address_refreshes
            .sync(lane, needs_refresh)
            .map_err(|_| BrokerSetError::SchedulerCapacityReached)?;
        self.runnable_lanes
            .sync(lane, needs_turn)
            .map_err(|_| BrokerSetError::SchedulerCapacityReached)?;
        self.reusable_lanes
            .sync(lane, is_reusable)
            .map_err(|_| BrokerSetError::SchedulerCapacityReached)?;
        self.deadlines
            .sync(lane, deadline)
            .map_err(|_| BrokerSetError::SchedulerCapacityReached)?;
        Ok(())
    }

    pub(super) fn remove_lane_indexes(&mut self, lane: BrokerLane) {
        self.address_refreshes.remove(lane);
        self.runnable_lanes.remove(lane);
        self.reusable_lanes.remove(lane);
        self.deadlines.remove(lane);
    }
}
