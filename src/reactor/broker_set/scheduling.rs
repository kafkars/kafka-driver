//! Exact runnable-index synchronization after one broker-lane mutation.

use super::{BrokerLane, BrokerSet, BrokerSetError};

impl BrokerSet {
    pub(super) fn sync_address_refresh(&mut self, lane: BrokerLane) -> Result<(), BrokerSetError> {
        let needs_refresh = self
            .child_index(lane)
            .and_then(|index| self.children.get(index))
            .is_some_and(|child| child.needs_address_refresh());
        if needs_refresh {
            self.address_refreshes
                .push(lane)
                .map_err(|_| BrokerSetError::SchedulerCapacityReached)?;
        } else {
            self.address_refreshes.remove(lane);
        }
        Ok(())
    }

    pub(super) fn remove_lane_indexes(&mut self, lane: BrokerLane) {
        self.address_refreshes.remove(lane);
    }
}
