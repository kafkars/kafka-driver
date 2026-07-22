//! Bounded current-state projection for seed and sparse discovered-broker lanes.

use kafka_driver_core::BrokerResolutionState;

use crate::{BrokerLanePhase, BrokerLaneSnapshot, SeedSnapshot};

use super::{BrokerSet, child::BrokerChild};

impl BrokerSet {
    pub(in crate::reactor) fn seed_snapshot(&self) -> Option<SeedSnapshot> {
        self.seed
            .as_ref()
            .map(|seed| SeedSnapshot::new(seed.broker_state(), seed.state().phase()))
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
            self.waiting.len(),
            self.waiting.retained_bytes(),
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
