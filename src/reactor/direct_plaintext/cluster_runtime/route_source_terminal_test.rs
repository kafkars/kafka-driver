//! Terminal work in one external source cannot steal the other's selected admission turn.

use std::time::Duration;

use kafka_driver_core::{EffectId, OutcomeStamp};

use crate::{RequestError, TrafficClass};

use super::super::route_resolution::RouteResolutionProgress;
use super::super::route_test_support as support;
use super::source_test_support as fixture;
use crate::reactor::direct_plaintext::shared_set_fixture_test::response;

#[test]
fn terminal_seed_settlement_does_not_starve_ready_route_admission() {
    let mut ready = fixture::ready_route(41);
    let route_call = fixture::queue_ready_route(&mut ready.runtime, ready.lane, 30);
    let first_seed =
        fixture::queue_seed(&mut ready.runtime, 31, Duration::from_secs(5), fixture::NOW);
    let second_seed =
        fixture::queue_seed(&mut ready.runtime, 32, Duration::from_secs(5), fixture::NOW);
    ready.runtime.begin_seed_waiting_drain();
    ready.runtime.routes_first = false;

    ready
        .runtime
        .drive(fixture::NOW, &mut ready.causality)
        .unwrap_or_else(fixture::fail);
    assert_eq!(first_seed.try_result(), Some(Ok(Err(draining()))));
    assert!(second_seed.try_result().is_none());
    assert_eq!(ready.runtime.routes[&ready.lane].waiting.len(), 1);

    ready
        .runtime
        .drive(fixture::NOW, &mut ready.causality)
        .unwrap_or_else(fixture::fail);
    assert!(ready.runtime.routes[&ready.lane].waiting.is_empty());
    assert!(second_seed.try_result().is_none());
    ready
        .runtime
        .drive(fixture::NOW, &mut ready.causality)
        .unwrap_or_else(fixture::fail);
    assert_eq!(second_seed.try_result(), Some(Ok(Err(draining()))));
    assert_eq!(
        fixture::finish_live_call(
            &mut ready.runtime,
            &mut ready.causality,
            &route_call,
            fixture::NOW,
        ),
        response(41)
    );
    ready
        .server
        .join()
        .unwrap_or_else(|_| panic!("join terminal-seed route server"));
}

#[test]
fn terminal_route_settlement_does_not_starve_ready_seed_admission() {
    let mut ready = fixture::ready_seed(43);
    let seed_call =
        fixture::queue_seed(&mut ready.runtime, 40, Duration::from_secs(5), fixture::NOW);
    let route = fixture::install_test_directory(&mut ready.runtime);
    let (route_call, lane, dns) = fixture::queue_route(
        &mut ready.runtime,
        route,
        (
            41,
            TrafficClass::LongPoll,
            Duration::from_secs(5),
            Some(EffectId::from_raw(41)),
            fixture::NOW,
        ),
        &mut ready.causality,
    );
    let (second_route_call, _, _) = fixture::queue_route(
        &mut ready.runtime,
        route,
        (
            42,
            TrafficClass::LongPoll,
            Duration::from_secs(5),
            None,
            fixture::NOW,
        ),
        &mut ready.causality,
    );
    let progress = ready
        .runtime
        .complete_route_resolution(
            lane,
            support::success(&dns.unwrap_or_else(|| panic!("terminal-route DNS")), 9092),
            &support::CountingFactory::new(),
            fixture::NOW,
        )
        .unwrap_or_else(fixture::fail);
    let RouteResolutionProgress::Activated(owner) = progress else {
        panic!("terminal route must activate")
    };
    ready
        .runtime
        .access(owner)
        .unwrap_or_else(|| panic!("terminal-route access"))
        .begin_session_drain(fixture::NOW, &mut ready.causality)
        .unwrap_or_else(fixture::fail);
    let observed_at = OutcomeStamp::from_raw(91);
    ready.runtime.record_route_failure(lane, observed_at);
    ready.runtime.routes_first = true;

    ready
        .runtime
        .drive(fixture::NOW, &mut ready.causality)
        .unwrap_or_else(fixture::fail);
    assert_eq!(route_call.try_result(), Some(Ok(Err(fixture::closed()))));
    assert!(second_route_call.try_result().is_none());
    assert!(!ready.runtime.seed_waiting.is_empty());
    assert_eq!(
        ready.runtime.routes[&lane].route_failure_at,
        Some(observed_at)
    );

    ready
        .runtime
        .drive(fixture::NOW, &mut ready.causality)
        .unwrap_or_else(fixture::fail);
    assert!(ready.runtime.seed_waiting.is_empty());
    assert!(second_route_call.try_result().is_none());
    assert_eq!(
        ready.runtime.routes[&lane].route_failure_at,
        Some(observed_at)
    );

    ready
        .runtime
        .drive(fixture::NOW, &mut ready.causality)
        .unwrap_or_else(fixture::fail);
    assert_eq!(
        second_route_call.try_result(),
        Some(Ok(Err(fixture::closed())))
    );
    assert_eq!(
        ready.runtime.routes[&lane].route_failure_at,
        Some(observed_at)
    );
    assert_eq!(
        fixture::finish_live_call(
            &mut ready.runtime,
            &mut ready.causality,
            &seed_call,
            fixture::NOW,
        ),
        response(43)
    );
    ready
        .server
        .join()
        .unwrap_or_else(|_| panic!("join terminal-route seed server"));
}

fn draining() -> RequestError {
    RequestError::Rejected {
        failure: kafka_driver_core::CallFailure::Draining,
        delivery: kafka_driver_core::Delivery::NotSent,
    }
}
