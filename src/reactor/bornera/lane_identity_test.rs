//! Identity-allocation proofs for atomic endpoint families and non-reuse.

use std::collections::BTreeSet;

use super::{BorneraIdentityAllocator, BorneraIdentityError};

#[test]
fn one_endpoint_reserves_four_disjoint_lane_domains_atomically() {
    let mut identities = BorneraIdentityAllocator::new();
    let (endpoint, lanes) = identities
        .reserve_endpoint_lanes::<4>()
        .unwrap_or_else(|error| panic!("reserve endpoint lanes: {error}"));
    let (replacement_endpoint, [replacement]) = identities
        .reserve_endpoint_lanes::<1>()
        .unwrap_or_else(|error| panic!("reserve replacement lane: {error}"));

    assert_eq!(endpoint.get(), 1);
    assert!(lanes.iter().all(|owner| owner.endpoint() == endpoint));
    assert_eq!(unique(lanes.map(|owner| owner.lane().get())), 4);
    assert_eq!(unique(lanes.map(|owner| owner.connection().get())), 4);
    assert_eq!(unique(lanes.map(|owner| owner.timer().get())), 4);
    assert_ne!(replacement_endpoint, endpoint);
    assert!(lanes.iter().all(|owner| owner.lane() != replacement.lane()));
    assert!(
        lanes
            .iter()
            .all(|owner| owner.connection() != replacement.connection())
    );
    assert!(
        lanes
            .iter()
            .all(|owner| owner.timer() != replacement.timer())
    );
}

#[test]
fn failed_batch_reservation_advances_no_identity_domain() {
    let mut identities = BorneraIdentityAllocator::at(Some(7), Some(u32::MAX - 1));

    assert_eq!(
        identities.reserve_endpoint_lanes::<4>(),
        Err(BorneraIdentityError::LaneExhausted)
    );
    let (endpoint, lanes) = identities
        .reserve_endpoint_lanes::<2>()
        .unwrap_or_else(|error| panic!("reserve last complete lane family: {error}"));

    assert_eq!(endpoint.get(), 7);
    assert_eq!(
        lanes.map(|owner| owner.lane().get()),
        [u32::MAX - 1, u32::MAX]
    );
    assert_eq!(
        identities.reserve_endpoint_lanes::<1>(),
        Err(BorneraIdentityError::LaneExhausted)
    );
}

#[test]
fn empty_and_exhausted_endpoint_groups_fail_without_wrapping() {
    let mut identities = BorneraIdentityAllocator::at(Some(u64::MAX), Some(9));
    assert_eq!(
        identities.reserve_endpoint_lanes::<0>(),
        Err(BorneraIdentityError::EmptyLaneGroup)
    );
    let (endpoint, [lane]) = identities
        .reserve_endpoint_lanes::<1>()
        .unwrap_or_else(|error| panic!("reserve final endpoint: {error}"));

    assert_eq!(endpoint.get(), u64::MAX);
    assert_eq!(lane.lane().get(), 9);
    assert_eq!(
        identities.reserve_endpoint_lanes::<1>(),
        Err(BorneraIdentityError::EndpointExhausted)
    );
}

fn unique<T: Ord, const N: usize>(values: [T; N]) -> usize {
    values.into_iter().collect::<BTreeSet<_>>().len()
}
