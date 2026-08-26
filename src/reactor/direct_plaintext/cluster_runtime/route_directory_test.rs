//! Directory monotonicity, generation retention, and semantic-state reclamation.

use std::time::Duration;

use kafka_driver_core::{EffectId, OutcomeStamp};

use crate::{RequestError, TrafficClass, reactor::causality::CausalSequence};

use super::super::route_test_support as support;
use support::fail;

#[test]
fn older_directory_cannot_regress_installed_generation() {
    let mut runtime = support::runtime(1, 4, 1);
    let broker = support::broker(7);
    let endpoint = support::endpoint("broker.test", 9092);
    let newer = support::directory(2, broker, endpoint.clone(), 1);
    let older = support::directory(1, broker, endpoint, 1);

    assert!(runtime.install_directory(&newer).unwrap_or_else(fail));
    assert!(!runtime.install_directory(&older).unwrap_or_else(fail));
    assert_eq!(
        runtime
            .directory
            .as_ref()
            .map(kafka_driver_core::BrokerDirectory::generation),
        Some(newer.generation())
    );
}

#[test]
fn same_endpoint_generation_retains_waiters_and_clears_failure_stamp() {
    let mut runtime = support::runtime(1, 4, 1);
    let broker = support::broker(7);
    let endpoint = support::endpoint("broker.test", 9092);
    let first = support::directory(1, broker, endpoint.clone(), 1);
    runtime.install_directory(&first).unwrap_or_else(fail);
    let first_route = first
        .route_to(broker)
        .unwrap_or_else(|| panic!("first route"));
    let (call, request) = support::request(1, TrafficClass::Bulk, Duration::from_secs(5));
    let mut causality = CausalSequence::new();
    let (lane, _) = runtime
        .submit_route(
            first_route,
            Some(EffectId::from_raw(1)),
            request,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(fail)
        .unwrap_or_else(|| panic!("first DNS request"));
    runtime.record_route_failure(lane, OutcomeStamp::from_raw(9));

    let second = support::directory(2, broker, endpoint, 1);
    assert!(runtime.install_directory(&second).unwrap_or_else(fail));

    let state = runtime
        .routes
        .get(&lane)
        .unwrap_or_else(|| panic!("route state"));
    assert_eq!(state.waiting.len(), 1);
    assert_eq!(state.route_failure_at, None);
    assert_eq!(
        state.advertised.as_ref().map(|advertised| advertised.route),
        second.route_to(broker)
    );
    assert!(call.try_result().is_none());
}

#[test]
fn retired_semantic_only_state_releases_membership_capacity() {
    let mut runtime = support::runtime(1, 2, 1);
    let first_broker = support::broker(7);
    let first = support::directory(1, first_broker, support::endpoint("first.test", 9092), 1);
    runtime.install_directory(&first).unwrap_or_else(fail);
    let (first_call, first_request) =
        support::request(1, TrafficClass::Control, Duration::from_secs(5));
    runtime
        .submit_route(
            first
                .route_to(first_broker)
                .unwrap_or_else(|| panic!("first route")),
            Some(EffectId::from_raw(1)),
            first_request,
            support::NOW,
            &mut CausalSequence::new(),
        )
        .unwrap_or_else(fail);

    let second_broker = support::broker(8);
    let second = support::directory(2, second_broker, support::endpoint("second.test", 9093), 1);
    runtime.install_directory(&second).unwrap_or_else(fail);

    assert!(runtime.routes.is_empty());
    assert_eq!(
        first_call.try_result(),
        Some(Ok(Err(RequestError::RouteUnavailable)))
    );
    let (_, second_request) = support::request(2, TrafficClass::Control, Duration::from_secs(5));
    assert!(
        runtime
            .submit_route(
                second
                    .route_to(second_broker)
                    .unwrap_or_else(|| panic!("second route")),
                Some(EffectId::from_raw(2)),
                second_request,
                support::NOW,
                &mut CausalSequence::new(),
            )
            .unwrap_or_else(fail)
            .is_some()
    );
}
