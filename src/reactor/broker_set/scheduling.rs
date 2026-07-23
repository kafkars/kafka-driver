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
        let deadline = child.next_deadline();
        if needs_refresh {
            self.address_refreshes
                .push(lane)
                .map_err(|_| BrokerSetError::SchedulerCapacityReached)?;
        } else {
            self.address_refreshes.remove(lane);
        }
        self.deadlines
            .sync(lane, deadline)
            .map_err(|_| BrokerSetError::SchedulerCapacityReached)?;
        Ok(())
    }

    pub(super) fn remove_lane_indexes(&mut self, lane: BrokerLane) {
        self.address_refreshes.remove(lane);
        self.deadlines.remove(lane);
    }
}
