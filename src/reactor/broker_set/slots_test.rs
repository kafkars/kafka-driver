//! Sparse-slot reclamation scenarios for adjacent terminal broker lanes.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::{BrokerDirectoryLimits, BrokerId, Moment};

use crate::{
    MetadataLimits, TrafficClass,
    reactor::{Poller, broker::BrokerLimits},
};

use super::{BrokerLane, BrokerSet};

#[test]
fn bounded_runnable_turns_reclaim_adjacent_sparse_slots_without_skipping() {
    let mut brokers = BrokerSet::new(
        BrokerLimits::default(),
        MetadataLimits::new(
            BrokerDirectoryLimits::new(nonzero(4)),
            Duration::from_secs(1),
        )
        .with_lane_turn_budget(NonZeroUsize::MIN),
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
        brokers
            .sync_lane(lane)
            .unwrap_or_else(|error| panic!("index retired lane: {error}"));
    }
    let poller =
        Poller::new(NonZeroUsize::MIN).unwrap_or_else(|error| panic!("build poller: {error}"));

    for remaining in (0..4).rev() {
        assert!(brokers.has_local_io());
        assert!(
            brokers
                .continue_io(
                    &poller,
                    Moment::ORIGIN,
                    kafka_driver_core::OutcomeStamp::ORIGIN,
                )
                .unwrap_or_else(|error| panic!("reclaim one broker lane: {error}"))
        );
        assert_eq!(brokers.allocated_lanes(), remaining);
    }

    assert!(!brokers.has_local_io());
    assert_eq!(brokers.free_slots.len(), 4);
    assert!(brokers.active_positions.iter().all(Option::is_none));
}

fn broker_id(value: i32) -> BrokerId {
    BrokerId::new(value).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}
