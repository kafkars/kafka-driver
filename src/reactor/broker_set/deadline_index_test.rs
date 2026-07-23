//! Exact replacement, capacity, and equal-deadline fairness index scenarios.

use std::num::NonZeroUsize;

use kafka_driver_core::{BrokerId, Moment};

use crate::TrafficClass;

use super::{BrokerLane, deadline_index::DeadlineIndex};

#[test]
fn updated_lane_owns_only_its_latest_deadline_and_removal_releases_capacity() {
    let mut deadlines = DeadlineIndex::new(NonZeroUsize::MIN);
    let lane = lane(1);

    assert!(deadlines.sync(lane, Some(moment(20))).is_ok());
    assert!(deadlines.sync(lane, Some(moment(10))).is_ok());
    assert_eq!(deadlines.len(), 1);
    assert_eq!(deadlines.next_deadline(), Some(moment(10)));
    assert_eq!(deadlines.take_due(moment(9)), None);
    assert_eq!(deadlines.take_due(moment(10)), Some(lane));
    assert_eq!(deadlines.next_deadline(), None);
}

#[test]
fn reinserted_hot_lane_follows_other_lanes_with_the_same_due_time() {
    let mut deadlines = DeadlineIndex::new(nonzero(2));
    let first = lane(1);
    let second = lane(2);
    assert!(deadlines.sync(first, Some(moment(10))).is_ok());
    assert!(deadlines.sync(second, Some(moment(10))).is_ok());

    assert_eq!(deadlines.take_due(moment(10)), Some(first));
    assert!(deadlines.sync(first, Some(moment(10))).is_ok());

    assert_eq!(deadlines.take_due(moment(10)), Some(second));
    assert_eq!(deadlines.take_due(moment(10)), Some(first));
}

#[test]
fn exact_capacity_rejects_an_unindexed_lane_without_disturbing_current_owner() {
    let mut deadlines = DeadlineIndex::new(NonZeroUsize::MIN);
    let admitted = lane(1);
    assert!(deadlines.sync(admitted, Some(moment(10))).is_ok());

    assert!(deadlines.sync(lane(2), Some(moment(5))).is_err());

    assert_eq!(deadlines.next_deadline(), Some(moment(10)));
    assert_eq!(deadlines.take_due(moment(10)), Some(admitted));
}

fn lane(raw_broker: i32) -> BrokerLane {
    BrokerLane::new(
        BrokerId::new(raw_broker).unwrap_or_else(|error| panic!("valid broker ID: {error}")),
        TrafficClass::Interactive,
    )
}

const fn moment(nanos: u64) -> Moment {
    Moment::from_nanos(nanos)
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test capacity must be nonzero"))
}
