//! Scenarios for legacy and batched coordinator response validation.

use kafka_driver_core::{CoordinatorKey, CoordinatorKind};
use kafka_wire::{FindCoordinatorResponse, find_coordinator_response::Coordinator};
use kafka_wire_core::{ApiVersion, StrBytes};

use super::{CoordinatorBuildError, coordinator_target};

#[test]
fn legacy_success_becomes_a_validated_broker_endpoint() {
    let mut response = FindCoordinatorResponse::default();
    response.node_id = 7;
    response.host = StrBytes::from("broker.test");
    response.port = 9_092;

    let (broker, endpoint) = coordinator_target(&response, &key(), version(3))
        .unwrap_or_else(|error| panic!("valid legacy response rejected: {error}"));

    assert_eq!(broker.get(), 7);
    assert_eq!(endpoint.host().as_str(), "broker.test");
    assert_eq!(endpoint.port().get(), 9_092);
}

#[test]
fn batched_success_requires_one_result_for_the_exact_key() {
    let mut response = FindCoordinatorResponse::default();
    response.coordinators.push(coordinator("payments", 7));

    assert_eq!(
        coordinator_target(&response, &key(), version(4)),
        Err(CoordinatorBuildError::KeyMismatch)
    );
    response.coordinators[0].key = StrBytes::from("orders");
    assert!(coordinator_target(&response, &key(), version(4)).is_ok());
    response.coordinators.push(coordinator("orders", 9));
    assert_eq!(
        coordinator_target(&response, &key(), version(4)),
        Err(CoordinatorBuildError::ResponseCount { observed: 2 })
    );
}

#[test]
fn invalid_host_is_rejected_without_retaining_advertised_text() {
    let rejected = format!("private-{}", "x".repeat(300));
    let mut response = FindCoordinatorResponse::default();
    response.coordinators.push(coordinator(&rejected, 7));
    response.coordinators[0].key = StrBytes::from("orders");
    response.coordinators[0].host = StrBytes::from(rejected.clone());

    let error = coordinator_target(&response, &key(), version(4))
        .err()
        .unwrap_or_else(|| panic!("invalid host must be rejected"));

    assert!(matches!(error, CoordinatorBuildError::BrokerHost(_)));
    assert!(!format!("{error:?}").contains(&rejected));
}

fn coordinator(raw_key: &str, node_id: i32) -> Coordinator {
    let mut coordinator = Coordinator::default();
    coordinator.key = StrBytes::from(raw_key);
    coordinator.node_id = node_id;
    coordinator.host = StrBytes::from("broker.test");
    coordinator.port = 9_092;
    coordinator
}

fn key() -> CoordinatorKey {
    CoordinatorKey::new(CoordinatorKind::Group, "orders")
        .unwrap_or_else(|error| panic!("valid key rejected: {error}"))
}

const fn version(value: i16) -> ApiVersion {
    ApiVersion::new(value)
}
