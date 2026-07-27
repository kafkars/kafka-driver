//! Queued coordinator discovery ownership before broker readiness.

use kafka_driver_core::{CoordinatorKey, CoordinatorKind};

use crate::CoordinatorLimits;

use super::CoordinatorOwner;

#[test]
fn requested_discovery_remains_pending_before_it_can_start() {
    let key = CoordinatorKey::new(CoordinatorKind::Group, "orders-readers")
        .unwrap_or_else(|error| panic!("valid coordinator key rejected: {error}"));
    let mut owner = CoordinatorOwner::new(CoordinatorLimits::default());
    let index = owner
        .entry_or_insert(key.clone())
        .unwrap_or_else(|| panic!("first coordinator key must fit"));

    owner.entries[index].discovery_requested = true;

    assert!(owner.discovery_pending(&key));
}
