//! Exact metadata-route invalidation through the deterministic generation fence.

use kafka_driver_core::{BrokerRoute, MetadataDisposition, MetadataInput, Moment, PartitionRoute};

use crate::{
    api::CallIds,
    reactor::{Poller, broker::SingleBroker},
};

use super::{MetadataOwner, MetadataOwnerError};

impl MetadataOwner {
    pub(in crate::reactor) fn invalidate_broker_route(
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

    pub(in crate::reactor) fn invalidate_partition_route(
        &mut self,
        route: PartitionRoute,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
    ) -> Result<MetadataDisposition, MetadataOwnerError> {
        let operation_id = self.reserve_operation()?;
        let transition = self.machine.apply(MetadataInput::InvalidatePartitionRoute {
            route,
            operation_id,
        });
        let disposition = transition.disposition();
        self.interpret(transition, broker, poller, now, call_ids)?;
        Ok(disposition)
    }
}
