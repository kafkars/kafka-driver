//! Exact metadata-route invalidation through the deterministic generation fence.

use kafka_driver_core::{BrokerRoute, MetadataDisposition, MetadataInput, Moment};

use crate::{
    api::CallIds,
    reactor::{Poller, broker::SingleBroker},
};

use super::{MetadataOwner, MetadataOwnerError};

impl MetadataOwner {
    pub(in crate::reactor) fn invalidate(
        &mut self,
        route: BrokerRoute,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
    ) -> Result<MetadataDisposition, MetadataOwnerError> {
        let operation_id = self.reserve_operation()?;
        let transition = self.machine.apply(MetadataInput::InvalidateBrokerRoute {
            route,
            operation_id,
        });
        let disposition = transition.disposition();
        self.interpret(transition, broker, poller, now, call_ids)?;
        Ok(disposition)
    }
}
