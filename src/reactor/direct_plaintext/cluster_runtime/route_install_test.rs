//! Endpoint replacement, sibling activation, and waiter-retention proofs.

use std::time::Duration;

use bornera_core::ConnectionEpoch as BorneraEpoch;
use kafka_driver_core::Moment;

use crate::TrafficClass;
use crate::reactor::route_waiting::RouteWaitingOutcome;

use super::super::route_test_support as support;
use super::{scenario_test, test_support as fixture};
use support::fail;

#[test]
fn changed_endpoint_replaces_sparse_family_and_preserves_fifo_and_deadlines() {
    let mut scenario = scenario_test::sparse_replacement();
    let family = &scenario.runtime.families[&scenario.broker];
    assert_eq!(family.endpoint(), &scenario.new_endpoint);
    assert!(!family.is_retiring());
    assert!(
        scenario
            .old_owners
            .iter()
            .all(|owner| !scenario.new_owners.contains(owner))
    );
    assert!(
        scenario
            .old_owners
            .iter()
            .all(|owner| scenario.runtime.view(*owner).is_none())
    );
    assert!(scenario.runtime.view(scenario.new_owners[0]).is_some());
    assert!(scenario.runtime.view(scenario.new_owners[3]).is_some());
    assert!(
        scenario.new_owners[1..3]
            .iter()
            .all(|owner| scenario.runtime.view(*owner).is_none())
    );
    assert_eq!(scenario.replacement.attempts.get(), 2);
    assert_eq!(
        scenario.replacement.physical_epochs(),
        vec![BorneraEpoch::new(1); 2]
    );
    assert!(
        scenario.runtime.routes[&scenario.control]
            .pending_install
            .is_none()
    );
    assert!(
        scenario.runtime.routes[&scenario.long_poll]
            .pending_install
            .is_none()
    );

    let state = scenario
        .runtime
        .routes
        .get_mut(&scenario.control)
        .unwrap_or_else(|| panic!("replacement control route"));
    for expected in [2, 5] {
        let RouteWaitingOutcome::Ready(request) = state.waiting.pop(support::NOW, None) else {
            panic!("control waiter must remain ready")
        };
        assert_eq!(request.call_id().get(), expected);
        request.fail(fixture::closed());
    }
    assert_eq!(
        scenario.new_call.try_result(),
        Some(Ok(Err(fixture::closed())))
    );
    assert_eq!(
        scenario.newer_call.try_result(),
        Some(Ok(Err(fixture::closed())))
    );
    let expiration = scenario
        .runtime
        .routes
        .get_mut(&scenario.long_poll)
        .unwrap_or_else(|| panic!("replacement long-poll route"))
        .waiting
        .expire_due_bounded(Moment::from_nanos(10), None, 8);
    assert_eq!(expiration.settled(), 1);
    assert_eq!(
        scenario.deadline_call.try_result(),
        Some(Ok(Err(fixture::deadline_exceeded())))
    );
    assert!(scenario.long_call.try_result().is_none());
}

#[test]
fn expired_lane_keeps_pending_evidence_without_spinning_and_reuses_it_lazily() {
    let mut scenario = scenario_test::lazy_pending();
    assert_eq!(scenario.replacement.attempts.get(), 1);
    assert!(
        scenario.runtime.routes[&scenario.control]
            .pending_install
            .is_some()
    );
    assert!(!scenario.runtime.route_install_has_local_work());
    assert!(
        !scenario
            .runtime
            .drive_route_installs(
                &scenario.replacement,
                Moment::from_nanos(10),
                &mut scenario.causality,
            )
            .unwrap_or_else(fail)
    );

    let (lazy_call, lazy) = support::request(13, TrafficClass::Control, Duration::from_secs(5));
    assert!(
        scenario
            .runtime
            .submit_route(
                scenario.route,
                None,
                lazy,
                Moment::from_nanos(10),
                &mut scenario.causality,
            )
            .unwrap_or_else(fail)
            .is_none()
    );
    assert!(scenario.runtime.route_install_has_local_work());
    assert!(
        scenario
            .runtime
            .drive_route_installs(
                &scenario.replacement,
                Moment::from_nanos(10),
                &mut scenario.causality,
            )
            .unwrap_or_else(fail)
    );
    assert_eq!(scenario.replacement.attempts.get(), 2);
    assert!(
        scenario.runtime.routes[&scenario.control]
            .pending_install
            .is_none()
    );
    let owner = fixture::owners(&scenario.runtime, scenario.broker)[0];
    assert!(scenario.runtime.view(owner).is_some());
    assert!(lazy_call.try_result().is_none());
}
