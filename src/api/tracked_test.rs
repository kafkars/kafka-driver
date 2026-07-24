//! Scenarios for exact route publication and routed result observation.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU16, NonZeroUsize},
    time::Duration,
};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, MetadataGeneration, OutcomeStamp,
};
use kafka_wire_core::ApiVersion;

use crate::{Call, SubmitError, TurnOutcome, completion::completion_pair};

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
fn foreign_driver_rejects_token_before_mailbox_admission() {
    let (issuing, _issuing_reactor) = super::Driver::builder()
        .broker(address(9092))
        .build_reactor()
        .unwrap_or_else(|error| panic!("issuing driver: {error}"));
    let (foreign, mut foreign_reactor) = super::Driver::builder()
        .broker(address(9093))
        .build_reactor()
        .unwrap_or_else(|error| panic!("foreign driver: {error}"));
    let token = controller_token_for(7, issuing.identity);

    assert!(matches!(
        foreign.invalidate(token),
        Err(SubmitError::ForeignDriver)
    ));
    let turn = foreign_reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("empty foreign turn: {error}"));
    assert!(!matches!(
        turn,
        TurnOutcome::Progress { commands, .. } | TurnOutcome::Shutdown { commands }
            if commands != 0
    ));
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

const fn address(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}
