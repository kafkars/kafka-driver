//! Exact broker and semantic traffic identity for one physical connection lane.

use std::cmp::Ordering;

use kafka_driver_core::BrokerId;

use crate::TrafficClass;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::reactor) struct BrokerLane {
    broker_id: BrokerId,
    traffic_class: TrafficClass,
}

impl Ord for BrokerLane {
    fn cmp(&self, other: &Self) -> Ordering {
        self.broker_id.cmp(&other.broker_id).then_with(|| {
            self.traffic_class
                .stable_order()
                .cmp(&other.traffic_class.stable_order())
        })
    }
}

impl PartialOrd for BrokerLane {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
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

    pub(in crate::reactor) const fn traffic_class(self) -> TrafficClass {
        self.traffic_class
    }
}
