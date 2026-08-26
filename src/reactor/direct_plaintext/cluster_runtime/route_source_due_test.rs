//! Due work in one external source cannot steal the other's selected admission turn.

use std::time::Duration;

use kafka_driver_core::{EffectId, Moment};

use crate::{RequestError, TrafficClass};

use super::source_test_support as fixture;
use crate::reactor::direct_plaintext::shared_set_fixture_test::response;

const DUE: Moment = Moment::from_nanos(10);

#[test]
fn continuously_due_seed_expiry_does_not_starve_ready_route_admission() {
    let mut ready = fixture::ready_route(31);
    let route_call = fixture::queue_ready_route(&mut ready.runtime, ready.lane, 10);
    let first_seed = fixture::queue_seed(
        &mut ready.runtime,
        11,
        Duration::from_nanos(10),
        Moment::ORIGIN,
    );
    let second_seed = fixture::queue_seed(
        &mut ready.runtime,
        12,
        Duration::from_nanos(10),
        Moment::ORIGIN,
    );
    ready.runtime.routes_first = true;

    assert!(
        ready
            .runtime
            .drive(DUE, &mut ready.causality)
            .unwrap_or_else(fixture::fail)
    );
    assert!(ready.runtime.routes[&ready.lane].waiting.is_empty());
    assert!(route_call.try_result().is_none());
    assert!(first_seed.try_result().is_none());
    assert!(second_seed.try_result().is_none());
    assert_eq!(ready.runtime.next_deadline(), Some(DUE));

    ready
        .runtime
        .drive(DUE, &mut ready.causality)
        .unwrap_or_else(fixture::fail);
    assert_eq!(first_seed.try_result(), Some(Ok(Err(deadline_exceeded()))));
    assert!(second_seed.try_result().is_none());
    ready
        .runtime
        .drive(DUE, &mut ready.causality)
        .unwrap_or_else(fixture::fail);
    assert_eq!(second_seed.try_result(), Some(Ok(Err(deadline_exceeded()))));
    assert_eq!(
        fixture::finish_live_call(&mut ready.runtime, &mut ready.causality, &route_call, DUE,),
        response(31)
    );
    ready
        .server
        .join()
        .unwrap_or_else(|_| panic!("join ready-route server"));
}

#[test]
fn continuously_due_route_expiry_does_not_starve_ready_seed_admission() {
    let mut ready = fixture::ready_seed(37);
    let seed_call =
        fixture::queue_seed(&mut ready.runtime, 20, Duration::from_secs(5), fixture::NOW);
    let route = fixture::install_test_directory(&mut ready.runtime);
    let (first_route, lane, _) = fixture::queue_route(
        &mut ready.runtime,
        route,
        (
            21,
            TrafficClass::Bulk,
            Duration::from_nanos(10),
            Some(EffectId::from_raw(21)),
            Moment::ORIGIN,
        ),
        &mut ready.causality,
    );
    let (second_route, _, _) = fixture::queue_route(
        &mut ready.runtime,
        route,
        (
            22,
            TrafficClass::Bulk,
            Duration::from_nanos(10),
            None,
            Moment::ORIGIN,
        ),
        &mut ready.causality,
    );
    ready.runtime.routes_first = false;

    ready
        .runtime
        .drive(DUE, &mut ready.causality)
        .unwrap_or_else(fixture::fail);
    assert!(ready.runtime.seed_waiting.is_empty());
    assert_eq!(ready.runtime.routes[&lane].waiting.len(), 2);
    assert!(first_route.try_result().is_none());
    assert!(second_route.try_result().is_none());
    assert_eq!(ready.runtime.next_deadline(), Some(DUE));

    ready
        .runtime
        .drive(DUE, &mut ready.causality)
        .unwrap_or_else(fixture::fail);
    assert_eq!(first_route.try_result(), Some(Ok(Err(deadline_exceeded()))));
    assert!(second_route.try_result().is_none());
    ready
        .runtime
        .drive(DUE, &mut ready.causality)
        .unwrap_or_else(fixture::fail);
    assert_eq!(
        second_route.try_result(),
        Some(Ok(Err(deadline_exceeded())))
    );
    assert_eq!(
        fixture::finish_live_call(&mut ready.runtime, &mut ready.causality, &seed_call, DUE,),
        response(37)
    );
    ready
        .server
        .join()
        .unwrap_or_else(|_| panic!("join ready-seed server"));
}

fn deadline_exceeded() -> RequestError {
    RequestError::Rejected {
        failure: kafka_driver_core::CallFailure::DeadlineExceeded,
        delivery: kafka_driver_core::Delivery::NotSent,
    }
}
