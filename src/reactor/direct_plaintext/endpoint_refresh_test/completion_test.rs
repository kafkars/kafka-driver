//! Refreshed address installation and resumed reconnect proofs.

use std::net::SocketAddr;

use kafka_driver_core::{AddressRefreshState, BrokerState, ConnectionEpoch, Moment};

use super::support::{RefreshFixture, addresses, new_addresses, old_addresses, reconnect_deadline};
use crate::reactor::causality::CausalSequence;

#[test]
fn success_before_original_deadline_preserves_it_then_opens_a_fresh_candidate() {
    let mut fixture = RefreshFixture::pending(91, 11);
    let refresh = fixture.take();
    let deadline = reconnect_deadline(&fixture.lane);
    let before = Moment::from_nanos(deadline.as_nanos() - 1);

    fixture
        .set
        .access(&mut fixture.lane)
        .finish_endpoint_refresh(
            &refresh,
            addresses(new_addresses()),
            before,
            &mut CausalSequence::new(),
        )
        .unwrap_or_else(|error| panic!("finish early endpoint refresh: {error}"));

    assert!(matches!(
        fixture.lane.lifecycle.state(),
        BrokerState::Backoff {
            failed_epoch,
            deadline: preserved,
            ..
        } if failed_epoch == refresh.failed_epoch() && preserved == deadline
    ));
    assert!(fixture.lane.endpoint_refresh.is_none());
    assert_eq!(fixture.seen(), old_addresses());
    assert!(
        fixture
            .set
            .access(&mut fixture.lane)
            .fire_due_reconnect(deadline, &mut CausalSequence::new())
            .unwrap_or_else(|error| panic!("open after preserved reconnect deadline: {error}"))
    );
    assert_eq!(fixture.seen(), expected_seen(new_addresses()[0]));
}

#[test]
fn success_at_original_deadline_replaces_rotation_before_immediate_open() {
    let mut fixture = RefreshFixture::pending(92, 12);
    let refresh = fixture.take();
    let deadline = reconnect_deadline(&fixture.lane);

    fixture
        .set
        .access(&mut fixture.lane)
        .finish_endpoint_refresh(
            &refresh,
            addresses(new_addresses()),
            deadline,
            &mut CausalSequence::new(),
        )
        .unwrap_or_else(|error| panic!("finish due endpoint refresh: {error}"));

    assert_eq!(fixture.seen(), expected_seen(new_addresses()[0]));
    assert!(matches!(
        fixture.lane.lifecycle.state(),
        BrokerState::Backoff {
            failed_epoch,
            next_epoch,
            ..
        } if failed_epoch == ConnectionEpoch::from_raw(3)
            && next_epoch == ConnectionEpoch::from_raw(4)
    ));
    assert!(fixture.lane.endpoint_refresh.is_none());
}

#[test]
fn immediate_failure_of_fresh_singleton_publishes_a_new_exact_fence() {
    let mut fixture = RefreshFixture::pending(93, 13);
    let old_refresh = fixture.take();
    let deadline = reconnect_deadline(&fixture.lane);
    let fresh = SocketAddr::from(([127, 0, 0, 31], 9092));

    fixture
        .set
        .access(&mut fixture.lane)
        .finish_endpoint_refresh(
            &old_refresh,
            addresses([fresh]),
            deadline,
            &mut CausalSequence::new(),
        )
        .unwrap_or_else(|error| panic!("finish singleton endpoint refresh: {error}"));

    assert_eq!(fixture.seen(), expected_seen(fresh));
    assert!(matches!(
        fixture.lane.lifecycle.state(),
        BrokerState::Refreshing {
            failed_epoch,
            refresh: AddressRefreshState::Pending { .. },
            ..
        } if failed_epoch == ConnectionEpoch::from_raw(3)
    ));
    let fresh_refresh = fixture
        .lane
        .endpoint_refresh
        .as_ref()
        .unwrap_or_else(|| panic!("fresh singleton failure must publish another fence"));
    assert_eq!(fresh_refresh.owner(), old_refresh.owner());
    assert_eq!(fresh_refresh.endpoint(), old_refresh.endpoint());
    assert_eq!(fresh_refresh.failed_epoch(), ConnectionEpoch::from_raw(3));
}

fn expected_seen(fresh: SocketAddr) -> Vec<SocketAddr> {
    old_addresses().into_iter().chain([fresh]).collect()
}
