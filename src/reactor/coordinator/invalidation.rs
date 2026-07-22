//! Exact route-token invalidation before a newer coordinator discovery.

use kafka_driver_core::{CoordinatorDisposition, CoordinatorInput, CoordinatorRoute, Moment};

use crate::{
    api::CallIds,
    reactor::{Poller, broker::SingleBroker},
};

use super::{CoordinatorOwner, CoordinatorOwnerError};

impl CoordinatorOwner {
    pub(in crate::reactor) fn invalidate(
        &mut self,
        route: CoordinatorRoute,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
    ) -> Result<CoordinatorDisposition, CoordinatorOwnerError> {
        let Some(index) = self.entry_index(route.key()) else {
            return Ok(CoordinatorDisposition::IgnoredStale);
        };
        let operation_id = self.reserve_operation()?;
        let transition = self.entries[index]
            .machine
            .apply(CoordinatorInput::Invalidate {
                route,
                operation_id,
            });
        let disposition = transition.disposition();
        self.interpret(index, transition, broker, poller, now, call_ids)?;
        Ok(disposition)
    }
}
