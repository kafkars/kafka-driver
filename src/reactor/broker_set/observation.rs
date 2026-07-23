//! Bounded current-state projection for seed and sparse discovered-broker lanes.

use kafka_driver_core::BrokerResolutionState;

use crate::{
    BrokerLaneLoadSnapshot, BrokerLanePhase, BrokerLaneSnapshot, SeedSnapshot, WriteQueueSnapshot,
};

use super::{BrokerSet, child::BrokerChild};

impl BrokerSet {
    pub(in crate::reactor) fn seed_snapshot(&self) -> Option<SeedSnapshot> {
        self.seed.as_ref().map(|seed| {
            SeedSnapshot::new(
                seed.broker_state(),
                seed.state().phase(),
                seed.last_close_reason(),
                seed.write_queue_snapshot(),
            )
        })
    }

    pub(in crate::reactor) fn lane_snapshots(&self) -> Vec<BrokerLaneSnapshot> {
        self.active_slots
            .iter()
            .filter_map(|index| self.children.get(*index))
            .map(|child| child.snapshot())
            .collect()
    }
}

impl BrokerChild {
    fn snapshot(&self) -> BrokerLaneSnapshot {
        BrokerLaneSnapshot::new(
            self.lane.broker_id(),
            self.lane.traffic_class(),
            self.phase_snapshot(),
            self.last_dns_failure,
            self.connection
                .as_ref()
                .and_then(super::super::broker::SingleBroker::last_close_reason),
            BrokerLaneLoadSnapshot::new(
                self.waiting.len(),
                self.waiting.retained_bytes(),
                self.connection.as_ref().map_or_else(
                    WriteQueueSnapshot::default,
                    super::super::broker::SingleBroker::write_queue_snapshot,
                ),
            ),
        )
    }

    fn phase_snapshot(&self) -> BrokerLanePhase {
        if self.retired {
            return BrokerLanePhase::Retired;
        }
        if let Some(connection) = &self.connection {
            return BrokerLanePhase::Owned {
                broker: connection.broker_state(),
                connection: connection.state().phase(),
            };
        }
        if matches!(
            self.resolution.state(),
            BrokerResolutionState::Resolving { .. }
        ) {
            BrokerLanePhase::Resolving
        } else {
            BrokerLanePhase::Dormant
        }
    }
}
