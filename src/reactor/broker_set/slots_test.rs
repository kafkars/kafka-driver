//! Sparse-slot reclamation scenarios for adjacent terminal broker lanes.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::{BrokerDirectoryLimits, BrokerId};

use crate::{MetadataLimits, TrafficClass, reactor::broker::BrokerLimits};

use super::{BrokerLane, BrokerSet};

#[test]
fn adjacent_reusable_slots_are_reclaimed_without_skipping_swapped_entries() {
    let mut brokers = BrokerSet::new(
        BrokerLimits::default(),
        MetadataLimits::new(
            BrokerDirectoryLimits::new(nonzero(4)),
            Duration::from_secs(1),
        ),
        None,
    )
    .unwrap_or_else(|error| panic!("build sparse broker set: {error}"));
    let lanes = (1..=4)
        .map(|raw| BrokerLane::new(broker_id(raw), TrafficClass::Interactive))
        .collect::<Vec<_>>();
    for &lane in &lanes {
        brokers
            .child_mut_for_lane(lane)
            .unwrap_or_else(|error| panic!("allocate broker lane: {error}"));
    }
    for lane in lanes {
        brokers
            .child_mut_for_lane(lane)
            .unwrap_or_else(|error| panic!("find broker lane: {error}"))
            .retire();
    }

    let reclaimed = brokers
        .reclaim_reusable_children()
        .unwrap_or_else(|error| panic!("reclaim broker lanes: {error}"));

    assert!(reclaimed);
    assert_eq!(brokers.allocated_lanes(), 0);
    assert_eq!(brokers.free_slots.len(), 4);
    assert_eq!(brokers.admission_cursor, 0);
}

fn broker_id(value: i32) -> BrokerId {
    BrokerId::new(value).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}
