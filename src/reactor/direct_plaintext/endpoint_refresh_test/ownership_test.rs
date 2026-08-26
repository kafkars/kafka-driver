//! Stable refresh identity, exact fence, and request ownership proofs.

use kafka_driver_core::{AddressRefreshState, BrokerState, DnsFailure, EffectId};

use super::support::{RefreshFixture, START, addresses, endpoint, new_addresses};
use crate::reactor::causality::CausalSequence;

#[test]
fn request_echoes_logical_endpoint_epoch_and_stable_bornera_lane_owner() {
    let mut first = RefreshFixture::pending(41, 7);
    let mut second = RefreshFixture::pending(42, 8);
    let first_refresh = first.take();
    let second_refresh = second.take();

    assert_eq!(first_refresh.endpoint(), &endpoint());
    assert_eq!(first_refresh.failed_epoch(), second_refresh.failed_epoch());
    assert_ne!(first_refresh.owner(), second_refresh.owner());
    assert_eq!(first_refresh.owner().endpoint().get(), 41);
    assert_eq!(first_refresh.owner().lane().get(), 7);

    let request = first_refresh.request(EffectId::from_raw(99));
    assert_eq!(request.epoch(), first_refresh.failed_epoch());
    assert_eq!(request.effect_id(), EffectId::from_raw(99));
    assert_eq!(request.endpoint(), first_refresh.endpoint());
}

#[test]
fn deferral_requires_the_exact_lane_fence_and_restores_pending_ownership() {
    let mut first = RefreshFixture::pending(51, 3);
    let mut second = RefreshFixture::pending(52, 4);
    let first_refresh = first.take();
    let wrong_lane = second.take();

    let mismatch = first.lane.defer_endpoint_refresh(&wrong_lane);
    assert!(mismatch.is_err());
    assert!(matches!(
        first.lane.lifecycle.state(),
        BrokerState::Refreshing {
            refresh: AddressRefreshState::Resolving { .. },
            ..
        }
    ));

    first
        .lane
        .defer_endpoint_refresh(&first_refresh)
        .unwrap_or_else(|error| panic!("defer exact refresh fence: {error}"));
    assert!(first.lane.endpoint_refresh_needed());
    assert_eq!(first.take(), first_refresh);
}

#[test]
fn completion_rejects_a_different_lane_fence_and_clears_terminal_ownership() {
    let mut failed = RefreshFixture::pending(53, 5);
    let mut finished = RefreshFixture::pending(54, 6);
    let mut wrong = RefreshFixture::pending(55, 7);
    let _failed_fence = failed.take();
    let _finished_fence = finished.take();
    let wrong_fence = wrong.take();

    let failure = failed.set.access(&mut failed.lane).fail_endpoint_refresh(
        &wrong_fence,
        DnsFailure::Temporary,
        START,
        &mut CausalSequence::new(),
    );
    assert!(failure.is_err());
    assert!(failed.lane.is_terminal());
    assert!(failed.lane.endpoint_refresh.is_none());

    let completion = finished
        .set
        .access(&mut finished.lane)
        .finish_endpoint_refresh(
            &wrong_fence,
            addresses(new_addresses()),
            START,
            &mut CausalSequence::new(),
        );
    assert!(completion.is_err());
    assert!(finished.lane.is_terminal());
    assert!(finished.lane.endpoint_refresh.is_none());
}
