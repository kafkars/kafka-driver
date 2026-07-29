//! Scenarios for exact route publication and routed result observation.

use std::num::{NonZeroU16, NonZeroUsize};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, MetadataGeneration, OutcomeStamp,
};
use kafka_wire_core::ApiVersion;

use crate::{Call, completion::completion_pair};

use super::{RouteFact, RouteKind, RoutedCall, RoutedOutcome, identity::DriverIdentity};

#[test]
fn completed_request_returns_the_exact_route_published_before_it() {
    let token = controller_token(7);
    let (receiver, completion) = completion_pair();
    let call = RoutedCall::new(Call::new(receiver));
    assert!(
        completion
            .complete(RoutedOutcome::new(
                Ok("response"),
                Some(ApiVersion::new(3)),
                Some(token),
            ))
            .is_ok()
    );

    let outcome = call
        .wait()
        .unwrap_or_else(|error| panic!("routed result must complete: {error}"));

    assert_eq!(outcome.result(), &Ok("response"));
    assert_eq!(outcome.selected_version(), Some(ApiVersion::new(3)));
    assert_eq!(
        outcome
            .route_failure_token()
            .map(super::RouteFailureToken::kind),
        Some(RouteKind::Controller)
    );
}

#[test]
fn routed_result_can_be_extracted_without_blocking() {
    let (receiver, completion) = completion_pair();
    let call = RoutedCall::new(Call::new(receiver));

    assert!(call.try_result().is_none());
    assert!(
        completion
            .complete(RoutedOutcome::new(
                Ok("response"),
                Some(ApiVersion::new(2)),
                None,
            ))
            .is_ok()
    );
    let Some(Ok(outcome)) = call.try_result() else {
        panic!("ready routed result must be extracted");
    };
    assert_eq!(outcome.result(), &Ok("response"));
    assert_eq!(outcome.selected_version(), Some(ApiVersion::new(2)));
    assert!(matches!(
        call.try_result(),
        Some(Err(crate::CompletionError::Consumed))
    ));
}

#[test]
fn exact_broker_route_publishes_its_distinct_invalidation_kind() {
    let broker_id = BrokerId::new(1).unwrap_or_else(|error| panic!("valid broker ID: {error}"));
    let host =
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid broker host: {error}"));
    let port = NonZeroU16::new(9092).unwrap_or_else(|| panic!("test port must be nonzero"));
    let directory = BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(7),
        [BrokerDirectoryEntry::new(
            broker_id,
            BrokerEndpoint::new(host, port),
        )],
        BrokerDirectoryLimits::new(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("valid broker directory: {error}"));
    let token = RouteFact::Broker(
        directory
            .route_to(broker_id)
            .unwrap_or_else(|| panic!("directory must issue broker route")),
    )
    .observe(
        DriverIdentity::allocate().unwrap_or_else(|| panic!("driver identity")),
        OutcomeStamp::from_raw(9),
    );

    assert_eq!(token.kind(), RouteKind::Broker);
}

fn controller_token(raw_generation: u64) -> super::RouteFailureToken {
    controller_token_for(
        raw_generation,
        DriverIdentity::allocate().unwrap_or_else(|| panic!("driver identity")),
    )
}

fn controller_token_for(raw_generation: u64, driver: DriverIdentity) -> super::RouteFailureToken {
    let broker_id = BrokerId::new(1).unwrap_or_else(|error| panic!("valid broker ID: {error}"));
    let host =
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid broker host: {error}"));
    let port = NonZeroU16::new(9092).unwrap_or_else(|| panic!("test port must be nonzero"));
    let directory = BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(raw_generation),
        [BrokerDirectoryEntry::new(
            broker_id,
            BrokerEndpoint::new(host, port),
        )],
        BrokerDirectoryLimits::new(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("valid broker directory: {error}"));
    RouteFact::Controller(
        directory
            .route_to(broker_id)
            .unwrap_or_else(|| panic!("directory must issue broker route")),
    )
    .observe(driver, OutcomeStamp::from_raw(9))
}
