//! FIFO, deduplication, removal, and capacity scenarios for lane work ownership.

use std::num::NonZeroUsize;

use kafka_driver_core::BrokerId;

use crate::TrafficClass;

use super::{BrokerLane, lane_queue::LaneQueue};

#[test]
fn exact_capacity_preserves_fifo_and_duplicate_pushes_are_idempotent() {
    let mut queue = LaneQueue::new(nonzero(2));
    let first = lane(1);
    let second = lane(2);

    assert_eq!(queue.push(first), Ok(true));
    assert_eq!(queue.push(first), Ok(false));
    assert_eq!(queue.push(second), Ok(true));
    assert!(queue.push(lane(3)).is_err());
    assert_eq!(queue.pop(), Some(first));
    assert_eq!(queue.pop(), Some(second));
    assert_eq!(queue.pop(), None);
}

#[test]
fn explicit_removal_releases_capacity_without_disturbing_fifo_order() {
    let mut queue = LaneQueue::new(nonzero(2));
    let first = lane(1);
    let second = lane(2);
    assert!(queue.push(first).is_ok());
    assert!(queue.push(second).is_ok());

    assert!(queue.remove(first));
    assert!(!queue.remove(first));
    assert!(queue.push(lane(3)).is_ok());

    assert_eq!(queue.pop(), Some(second));
    assert_eq!(queue.pop(), Some(lane(3)));
    assert!(queue.is_empty());
    assert_eq!(queue.len(), 0);
}

fn lane(raw_broker: i32) -> BrokerLane {
    BrokerLane::new(
        BrokerId::new(raw_broker).unwrap_or_else(|error| panic!("valid broker ID: {error}")),
        TrafficClass::Interactive,
    )
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test capacity must be nonzero"))
}
