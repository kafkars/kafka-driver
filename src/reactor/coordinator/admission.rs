//! Admission of public calls behind one exact coordinator-key discovery.

use kafka_driver_core::{EvidenceStamp, Moment};

use crate::{
    RequestError,
    api::CallIds,
    reactor::{Poller, broker::SingleBroker},
};

use super::{CoordinatorOwner, CoordinatorOwnerError, CoordinatorWait};

impl CoordinatorOwner {
    pub(in crate::reactor) fn wait_for(
        &mut self,
        waiting: CoordinatorWait,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<(), CoordinatorOwnerError> {
        let key = waiting.key().clone();
        let call_id = waiting.call_id();
        if !self.waiters.admit(waiting, now) {
            return Ok(());
        }
        let Some(index) = self.entry_or_insert(key) else {
            let request = self
                .waiters
                .retract_last(call_id)
                .ok_or(CoordinatorOwnerError::UnexpectedEffect)?;
            request.fail(RequestError::CoordinatorCapacityReached {
                limit: self.limits.keys().get(),
            });
            return Ok(());
        };
        if self.entries[index].machine.current().is_none() && self.entries[index].pending.is_none()
        {
            self.entries[index].discovery_requested = true;
        }
        self.start_requested(
            broker,
            poller,
            now,
            call_ids,
            evidence,
            self.limits.turn_budget().get(),
        )
        .map(|_| ())
    }
}
