//! Fair bounded progress for broker lanes with immediate local work.

use kafka_driver_core::{Moment, OutcomeStamp};

use crate::reactor::Poller;

use super::{BrokerSet, BrokerSetError};

impl BrokerSet {
    pub(super) fn continue_runnable_lanes(
        &mut self,
        poller: &Poller,
        now: Moment,
        observed_at: OutcomeStamp,
    ) -> Result<bool, BrokerSetError> {
        let mut progress = false;
        let mut lanes = 0;
        let mut admissions = 0;
        while lanes < self.lane_turn_budget.get() {
            let Some(lane) = self.runnable_lanes.pop() else {
                break;
            };
            lanes += 1;
            let Some(index) = self.child_index(lane) else {
                self.remove_lane_indexes(lane);
                continue;
            };
            progress |= self
                .children
                .get_mut(index)
                .ok_or(BrokerSetError::UnknownBrokerChild)?
                .continue_io(poller, now, observed_at)?;
            progress |= self.activate_child(index, poller, now)?;
            if admissions < self.admission_budget.get() {
                let admitted = self
                    .children
                    .get_mut(index)
                    .ok_or(BrokerSetError::UnknownBrokerChild)?
                    .admit_one(poller, now)?;
                progress |= admitted;
                admissions += usize::from(admitted);
            }
            let reusable = self
                .children
                .get(index)
                .ok_or(BrokerSetError::UnknownBrokerChild)?
                .is_reusable();
            if reusable {
                progress |= self.reclaim_lane(lane)?;
            } else {
                self.sync_lane(lane)?;
            }
        }
        Ok(progress)
    }
}
