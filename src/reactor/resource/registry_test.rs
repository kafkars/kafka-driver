//! Scenarios for resource bounds, ownership, lookup, removal, and stale tokens.

use std::num::NonZeroUsize;

use calandria::{ResourceGeneration, ResourceOwnerId, ResourceSlotId};
use kafka_driver_core::{ConnectionEpoch, TransportId};

use super::{
    ResourceAdmissionFailure, ResourceIdentity, ResourceNamespace, ResourceToken,
    registry::ResourceRegistry,
};

#[test]
fn admitted_resource_is_found_by_its_exact_token_and_identity() {
    let mut registry = registry(2);
    let expected_identity = identity(1, 10);

    let Ok(token) = registry.admit(expected_identity, String::from("socket-1")) else {
        panic!("resource must fit");
    };
    let Some((found_identity, resource)) = registry.get_mut(token) else {
        panic!("admitted token must resolve");
    };

    assert_eq!(found_identity, expected_identity);
    assert_eq!(resource, "socket-1");
    assert_eq!(registry.token_for(expected_identity), Some(token));
    assert_eq!(registry.len(), 1);
}

#[test]
fn duplicate_transport_identity_returns_the_unadmitted_resource() {
    let mut registry = registry(2);
    admit(&mut registry, identity(1, 10), "socket-1");

    let Err(error) = registry.admit(identity(1, 11), "socket-2") else {
        panic!("live transport identity must remain unique");
    };

    assert_eq!(
        error.failure(),
        ResourceAdmissionFailure::IdentityInUse {
            transport_id: transport(1),
        }
    );
    assert_eq!(error.into_resource(), "socket-2");
    assert_eq!(registry.len(), 1);
}

#[test]
fn capacity_rejection_returns_the_unadmitted_resource() {
    let mut registry = registry(1);
    admit(&mut registry, identity(1, 10), "socket-1");

    let Err(error) = registry.admit(identity(2, 10), "socket-2") else {
        panic!("second live resource must exceed capacity");
    };

    assert_eq!(
        error.failure(),
        ResourceAdmissionFailure::CapacityReached { limit: 1 }
    );
    assert_eq!(error.into_resource(), "socket-2");
    assert_eq!(registry.len(), 1);
}

#[test]
fn slot_reuse_invalidates_stale_readiness_tokens() {
    let mut registry = registry(1);
    let stale_token = admit(&mut registry, identity(1, 10), "socket-1");

    assert_eq!(
        registry.remove(stale_token),
        Some((identity(1, 10), "socket-1"))
    );
    let current_token = admit(&mut registry, identity(2, 11), "socket-2");

    assert_ne!(current_token, stale_token);
    assert!(registry.get_mut(stale_token).is_none());
    assert!(registry.remove(stale_token).is_none());
    assert!(registry.get_mut(current_token).is_some());
    assert_eq!(registry.len(), 1);
}

#[test]
fn mismatched_generation_cannot_remove_the_current_resource() {
    let mut registry = registry(2);
    let current = admit(&mut registry, identity(1, 10), "socket-1");
    let stale = ResourceToken::new(
        current.owner(),
        current.slot(),
        ResourceGeneration::new(current.generation().get() + 1),
    );

    assert!(registry.remove(stale).is_none());
    assert_eq!(
        registry.remove(current),
        Some((identity(1, 10), "socket-1"))
    );
}

#[test]
fn foreign_owner_cannot_name_or_remove_a_resource() {
    let mut registry = registry(1);
    let current = admit(&mut registry, identity(1, 10), "socket-1");

    let foreign = ResourceToken::new(
        ResourceOwnerId::new(1),
        ResourceSlotId::new(0),
        ResourceGeneration::INITIAL,
    );
    assert!(registry.get_mut(foreign).is_none());
    assert!(registry.remove(foreign).is_none());
    assert!(registry.get_mut(current).is_some());
}

#[test]
fn exhausted_generation_space_is_explicit_and_preserves_ownership() {
    let mut registry = ResourceRegistry::with_generation(nonzero(1), u64::MAX);
    let last_token = admit(&mut registry, identity(1, 10), "socket-1");
    assert_eq!(last_token.generation(), ResourceGeneration::MAX);
    assert_eq!(
        registry.remove(last_token),
        Some((identity(1, 10), "socket-1"))
    );

    let Err(error) = registry.admit(identity(2, 11), "socket-2") else {
        panic!("exhausted slot cannot be reused");
    };

    assert_eq!(
        error.failure(),
        ResourceAdmissionFailure::TokenSpaceExhausted
    );
    assert_eq!(error.into_resource(), "socket-2");
    assert_eq!(registry.len(), 0);
}

#[test]
fn broker_namespaces_make_equal_local_slots_and_generations_globally_disjoint() {
    let owners = nonzero(2);
    let left_namespace =
        ResourceNamespace::new(0, owners).unwrap_or_else(|| panic!("left namespace must fit"));
    let right_namespace =
        ResourceNamespace::new(1, owners).unwrap_or_else(|| panic!("right namespace must fit"));
    let mut left = ResourceRegistry::in_namespace(nonzero(1), left_namespace);
    let mut right = ResourceRegistry::in_namespace(nonzero(1), right_namespace);

    let left_token = admit(&mut left, identity(1, 1), "left");
    let right_token = admit(&mut right, identity(1, 1), "right");

    assert_ne!(left_token, right_token);
    assert_eq!(left_token.owner(), ResourceOwnerId::new(0));
    assert_eq!(right_token.owner(), ResourceOwnerId::new(1));
    assert!(left.get_mut(right_token).is_none());
    assert!(right.get_mut(left_token).is_none());
}

fn registry<R>(capacity: usize) -> ResourceRegistry<R> {
    ResourceRegistry::new(nonzero(capacity))
}

fn admit<R>(
    registry: &mut ResourceRegistry<R>,
    identity: ResourceIdentity,
    resource: R,
) -> ResourceToken {
    let Ok(token) = registry.admit(identity, resource) else {
        panic!("test resource must be admitted");
    };
    token
}

fn identity(transport_id: u64, epoch: u64) -> ResourceIdentity {
    ResourceIdentity::new(transport(transport_id), ConnectionEpoch::from_raw(epoch))
}

fn transport(raw: u64) -> TransportId {
    TransportId::from_raw(raw)
}

fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("test value must be nonzero");
    };
    value
}
