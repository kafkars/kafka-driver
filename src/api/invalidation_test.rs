//! Scenarios for ownership-preserving route-invalidation admission failure.

use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU16, NonZeroUsize},
    time::Duration,
};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, MetadataGeneration, OutcomeStamp,
};

use crate::{
    DriverLimits, SubmitError, TurnOutcome,
    completion::completion_pair,
    reactor::{Command, TrySendError},
};

use super::{RouteFact, RouteKind, identity::DriverIdentity};

#[test]
fn foreign_driver_rejection_returns_the_exact_token_before_admission() {
    let (issuing, _issuing_reactor) = super::Driver::builder()
        .broker(address(9092))
        .build_reactor()
        .unwrap_or_else(|error| panic!("issuing driver: {error}"));
    let (foreign, mut foreign_reactor) = super::Driver::builder()
        .broker(address(9093))
        .build_reactor()
        .unwrap_or_else(|error| panic!("foreign driver: {error}"));
    let token = controller_token(7, issuing.identity);

    let Err(rejection) = foreign.invalidate(token) else {
        panic!("foreign invalidation must be rejected");
    };

    assert!(matches!(rejection.reason(), SubmitError::ForeignDriver));
    let (reason, recovered) = rejection.into_parts();
    assert!(matches!(reason, SubmitError::ForeignDriver));
    assert_eq!(recovered.kind(), RouteKind::Controller);
    assert!(issuing.invalidate(recovered).is_ok());
    let turn = foreign_reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("empty foreign turn: {error}"));
    assert!(!matches!(
        turn,
        TurnOutcome::Progress { commands, .. } | TurnOutcome::Shutdown { commands }
            if commands != 0
    ));
}

#[test]
fn full_mailbox_rejection_returns_the_unadmitted_token() {
    let limits = DriverLimits::new(NonZeroUsize::MIN, NonZeroUsize::MIN);
    let (driver, _reactor) = super::Driver::builder()
        .limits(limits)
        .broker(address(9092))
        .build_reactor()
        .unwrap_or_else(|error| panic!("driver: {error}"));
    assert!(
        driver
            .invalidate(controller_token(7, driver.identity))
            .is_ok()
    );

    let Err(rejection) = driver.invalidate(controller_token(8, driver.identity)) else {
        panic!("second invalidation must exceed mailbox capacity");
    };

    assert!(matches!(rejection.reason(), SubmitError::Full));
    let (reason, recovered) = rejection.into_parts();
    assert!(matches!(reason, SubmitError::Full));
    assert_eq!(recovered.kind(), RouteKind::Controller);
}

#[test]
fn byte_capacity_rejection_returns_token_without_materializing_a_command() {
    let preview = controller_token(
        7,
        DriverIdentity::allocate().unwrap_or_else(|| panic!("preview driver identity")),
    );
    let Some(byte_capacity) =
        NonZeroUsize::new(Command::invalidation_retained_bytes(&preview).saturating_sub(1))
    else {
        panic!("an invalidation command retains more than one byte");
    };
    let limits =
        DriverLimits::new(nonzero(2), NonZeroUsize::MIN).with_mailbox_byte_capacity(byte_capacity);
    let (driver, mut reactor) = super::Driver::builder()
        .limits(limits)
        .broker(address(9092))
        .build_reactor()
        .unwrap_or_else(|error| panic!("driver: {error}"));

    let Err(rejection) = driver.invalidate(controller_token(7, driver.identity)) else {
        panic!("underweight byte capacity must reject invalidation");
    };

    assert!(matches!(rejection.reason(), SubmitError::Full));
    let (_reason, recovered) = rejection.into_parts();
    assert_eq!(recovered.kind(), RouteKind::Controller);
    let turn = reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("empty byte-rejection turn: {error}"));
    assert!(!matches!(
        turn,
        TurnOutcome::Progress { commands, .. } | TurnOutcome::Shutdown { commands }
            if commands != 0
    ));
}

#[test]
fn closed_mailbox_rejection_returns_the_unadmitted_token() {
    let (driver, reactor) = super::Driver::builder()
        .broker(address(9092))
        .build_reactor()
        .unwrap_or_else(|error| panic!("driver: {error}"));
    drop(reactor);

    let Err(rejection) = driver.invalidate(controller_token(7, driver.identity)) else {
        panic!("closed invalidation admission must fail");
    };

    assert!(matches!(rejection.reason(), SubmitError::Closed));
    let (reason, recovered) = rejection.into_parts();
    assert!(matches!(reason, SubmitError::Closed));
    assert_eq!(recovered.kind(), RouteKind::Controller);
}

#[test]
fn wake_rejection_preserves_both_source_and_token() {
    let token = controller_token(
        7,
        DriverIdentity::allocate().unwrap_or_else(|| panic!("driver identity")),
    );
    let rejection = super::invalidation::rejected_admission(TrySendError::Wake {
        command: token,
        source: io::Error::new(io::ErrorKind::BrokenPipe, "test wake failure"),
    });

    assert!(matches!(
        rejection.reason(),
        SubmitError::Wake(source) if source.kind() == io::ErrorKind::BrokenPipe
    ));
    let (reason, recovered) = rejection.into_parts();
    assert!(matches!(
        reason,
        SubmitError::Wake(source) if source.kind() == io::ErrorKind::BrokenPipe
    ));
    assert_eq!(recovered.kind(), RouteKind::Controller);
}

#[test]
fn retained_owner_weight_matches_the_materialized_invalidation_command() {
    let token = controller_token(
        7,
        DriverIdentity::allocate().unwrap_or_else(|| panic!("driver identity")),
    );
    let expected = Command::invalidation_retained_bytes(&token);
    let (_receiver, completion) = completion_pair();
    let command = Command::Invalidate { token, completion };

    assert_eq!(command.retained_bytes(), expected);
}

fn controller_token(raw_generation: u64, driver: DriverIdentity) -> super::RouteFailureToken {
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

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test value must be nonzero"))
}
