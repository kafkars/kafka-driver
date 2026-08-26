//! Ready gating, cross-source fairness, and incoming-request totality.

use std::time::Duration;

use bornera::TcpTransport;
use calandria::Span;
use kafka_driver_core::{CallFailure, Delivery, EffectId, Moment};

use crate::{RequestError, TrafficClass, reactor::causality::CausalSequence};

use super::{super::route_test_support as support, ClusterRuntime};
use crate::reactor::direct_plaintext::shared_set_fixture_test::{
    address, listener, response, spawn_lane,
};
use support::fail;

#[test]
fn budget_one_alternates_seed_and_route_deadline_service() {
    let mut runtime = support::runtime(1, 4, 1);
    let broker = support::broker(7);
    let directory = support::directory(1, broker, support::endpoint("broker.test", 9092), 1);
    runtime.install_directory(&directory).unwrap_or_else(fail);
    let mut causality = CausalSequence::new();
    let (seed_call, seed) = support::request(1, TrafficClass::Control, Duration::from_nanos(10));
    runtime
        .submit_seed(seed, Moment::ORIGIN, &mut causality)
        .unwrap_or_else(fail);
    let (route_call, request) = support::request(2, TrafficClass::Bulk, Duration::from_nanos(10));
    runtime
        .submit_route(
            directory
                .route_to(broker)
                .unwrap_or_else(|| panic!("route")),
            Some(EffectId::from_raw(1)),
            request,
            Moment::ORIGIN,
            &mut causality,
        )
        .unwrap_or_else(fail);

    assert!(
        runtime
            .drive(Moment::from_nanos(10), &mut causality)
            .unwrap_or_else(fail)
    );
    assert_eq!(seed_call.try_result(), Some(Ok(Err(deadline_exceeded()))));
    assert!(route_call.try_result().is_none());
    let (replacement_seed_call, replacement_seed) =
        support::request(3, TrafficClass::Control, Duration::from_nanos(10));
    runtime
        .submit_seed(replacement_seed, Moment::ORIGIN, &mut causality)
        .unwrap_or_else(fail);

    assert!(
        runtime
            .drive(Moment::from_nanos(10), &mut causality)
            .unwrap_or_else(fail)
    );
    assert_eq!(route_call.try_result(), Some(Ok(Err(deadline_exceeded()))));
    assert!(replacement_seed_call.try_result().is_none());
}

#[test]
fn host_fatal_totalizes_seed_and_every_discovered_waiter() {
    let mut runtime = support::runtime(1, 4, 1);
    let broker = support::broker(7);
    let directory = support::directory(1, broker, support::endpoint("broker.test", 9092), 1);
    runtime.install_directory(&directory).unwrap_or_else(fail);
    let mut causality = CausalSequence::new();
    let (seed_call, seed) = support::request(1, TrafficClass::Control, Duration::from_secs(5));
    runtime
        .submit_seed(seed, support::NOW, &mut causality)
        .unwrap_or_else(fail);
    let route = directory
        .route_to(broker)
        .unwrap_or_else(|| panic!("route"));
    let (bulk_call, bulk_request) = support::request(2, TrafficClass::Bulk, Duration::from_secs(5));
    let (bulk_lane, bulk_dns) = runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(1)),
            bulk_request,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(fail)
        .unwrap_or_else(|| panic!("bulk DNS"));
    let (long_poll_call, long_poll_request) =
        support::request(3, TrafficClass::LongPoll, Duration::from_secs(5));
    runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(2)),
            long_poll_request,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(fail);
    let factory = support::FailingFactory::new();

    assert!(
        runtime
            .complete_route_resolution(
                bulk_lane,
                support::success(&bulk_dns, 9092),
                &factory,
                support::NOW,
            )
            .is_err()
    );

    let expected = Some(Ok(Err(closed())));
    assert_eq!(seed_call.try_result(), expected.clone());
    assert_eq!(bulk_call.try_result(), expected.clone());
    assert_eq!(long_poll_call.try_result(), expected);
    assert_eq!(factory.attempts.get(), 1);
    assert!(runtime.seed_waiting.is_empty());
    assert!(
        runtime
            .routes
            .values()
            .all(|state| state.waiting.is_empty())
    );
}

#[test]
fn missing_resolution_permit_settles_incoming_and_totalizes_owned_waiters() {
    let mut runtime = support::runtime(1, 4, 1);
    let broker = support::broker(7);
    let directory = support::directory(1, broker, support::endpoint("broker.test", 9092), 1);
    runtime.install_directory(&directory).unwrap_or_else(fail);
    let route = directory
        .route_to(broker)
        .unwrap_or_else(|| panic!("route"));
    let mut causality = CausalSequence::new();
    let (owned_call, owned) = support::request(1, TrafficClass::Bulk, Duration::from_secs(5));
    runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(1)),
            owned,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(fail);
    let (incoming_call, incoming) =
        support::request(2, TrafficClass::LongPoll, Duration::from_secs(5));

    let error = runtime
        .submit_route(route, None, incoming, support::NOW, &mut causality)
        .err()
        .unwrap_or_else(|| panic!("missing permit must fail host"));

    assert_eq!(
        error.to_string(),
        "Bornera route resolution permit is missing"
    );
    assert_eq!(
        incoming_call.try_result(),
        Some(Ok(Err(RequestError::IdentityConflict)))
    );
    assert_eq!(owned_call.try_result(), Some(Ok(Err(closed()))));
}

#[test]
fn external_route_waits_until_physical_lane_is_ready() {
    let listener = listener();
    let address = address(&listener);
    let server = spawn_lane(listener, None, 23);
    let driver = support::driver(1, 2);
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver).unwrap_or_else(fail);
    let broker = support::broker(7);
    let directory = support::directory(
        1,
        broker,
        support::endpoint("broker.test", address.port()),
        1,
    );
    runtime.install_directory(&directory).unwrap_or_else(fail);
    let route = directory
        .route_to(broker)
        .unwrap_or_else(|| panic!("route"));
    let mut causality = CausalSequence::new();
    let (call, request) = support::request(1, TrafficClass::Interactive, Duration::from_secs(5));
    let (lane, dns) = runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(1)),
            request,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(fail)
        .unwrap_or_else(|| panic!("DNS"));
    let owner = match runtime
        .complete_route_resolution(
            lane,
            support::success(&dns, address.port()),
            &support::plaintext_factory(&driver),
            support::NOW,
        )
        .unwrap_or_else(fail)
    {
        super::super::route_resolution::RouteResolutionProgress::Activated(owner) => owner,
        progress => panic!("unexpected resolution progress: {progress:?}"),
    };
    let index = runtime.index(owner).unwrap_or_else(fail);
    assert_eq!(runtime.routes[&lane].waiting.len(), 1);
    assert!(runtime.lanes[index].pending.is_empty());

    let mut result = None;
    for _turn in 0..128 {
        runtime
            .drive(support::NOW, &mut causality)
            .unwrap_or_else(fail);
        result = result.or_else(|| call.try_result());
        if result.is_some() {
            break;
        }
        if !runtime.has_local_work() {
            runtime
                .wait(Span::try_from(Duration::from_millis(100)).unwrap_or(Span::ZERO))
                .unwrap_or_else(fail);
        }
    }

    assert_eq!(result, Some(Ok(Ok(response(23)))));
    assert!(runtime.routes[&lane].waiting.is_empty());
    assert!(runtime.lanes[index].pending.is_empty());
    server
        .join()
        .unwrap_or_else(|_| panic!("join route broker"));
}

fn closed() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Closed,
        delivery: Delivery::NotSent,
    }
}

fn deadline_exceeded() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::DeadlineExceeded,
        delivery: Delivery::NotSent,
    }
}
