//! Scenarios for exact DNS effect-owner capacity and identity removal.

use std::num::NonZeroUsize;

use kafka_driver_core::{BrokerId, EffectId};

use crate::{TrafficClass, reactor::broker_set::BrokerLane};

use super::{ResolutionOwner, ResolverOwnership, ResolverOwnershipError};

#[test]
fn exact_capacity_is_owned_and_one_more_effect_is_rejected() {
    let mut ownership = ResolverOwnership::new(nonzero(1));
    assert!(
        ownership
            .register(EffectId::from_raw(1), ResolutionOwner::Bootstrap)
            .is_ok()
    );

    let overflow = ownership.register(EffectId::from_raw(2), ResolutionOwner::Broker(lane(7)));

    assert_eq!(
        overflow,
        Err(ResolverOwnershipError::CapacityReached { limit: 1 })
    );
}

#[test]
fn completion_returns_the_exact_owner_and_releases_capacity() {
    let mut ownership = ResolverOwnership::new(nonzero(1));
    let owner = ResolutionOwner::Broker(lane(7));
    ownership
        .register(EffectId::from_raw(1), owner)
        .unwrap_or_else(|error| panic!("first owner must fit: {error}"));

    assert_eq!(ownership.remove(EffectId::from_raw(1)), Some(owner));
    assert_eq!(ownership.remove(EffectId::from_raw(1)), None);
    assert!(
        ownership
            .register(EffectId::from_raw(2), ResolutionOwner::Bootstrap)
            .is_ok()
    );
}

fn broker_id(raw: i32) -> BrokerId {
    BrokerId::new(raw).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

fn lane(raw: i32) -> BrokerLane {
    BrokerLane::new(broker_id(raw), TrafficClass::Interactive)
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test capacity must be nonzero"))
}
