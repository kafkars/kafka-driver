//! Exact broker and semantic traffic identity for one physical connection lane.

use kafka_driver_core::BrokerId;

use crate::TrafficClass;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::reactor) struct BrokerLane {
    broker_id: BrokerId,
    traffic_class: TrafficClass,
}

impl BrokerLane {
    pub(in crate::reactor) const fn new(broker_id: BrokerId, traffic_class: TrafficClass) -> Self {
        Self {
            broker_id,
            traffic_class,
        }
    }

    pub(in crate::reactor) const fn broker_id(self) -> BrokerId {
        self.broker_id
    }
}
