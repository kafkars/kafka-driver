//! Scenarios for seed-slot reservation and bounded broker namespace capacity.

use std::num::NonZeroUsize;

use kafka_driver_core::BrokerDirectoryLimits;

use crate::reactor::broker::BrokerLimits;

use super::{BrokerSet, BrokerSetError};

#[test]
fn discovered_broker_capacity_reserves_one_additional_seed_slot() {
    let set = BrokerSet::new(
        BrokerLimits::default(),
        BrokerDirectoryLimits::new(nonzero(7)),
    )
    .unwrap_or_else(|error| panic!("representable broker set: {error}"));

    assert_eq!(set.owner_capacity(), nonzero(8));
    assert!(!set.has_seed());
}

#[test]
fn maximum_directory_capacity_fails_before_token_namespace_wraparound() {
    let result = BrokerSet::new(
        BrokerLimits::default(),
        BrokerDirectoryLimits::new(NonZeroUsize::MAX),
    );

    assert!(matches!(result, Err(BrokerSetError::OwnerCapacityOverflow)));
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("test capacity must be nonzero");
    };
    value
}
