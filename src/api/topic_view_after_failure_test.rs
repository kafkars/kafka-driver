//! Ownership-preserving causal topic-view admission scenarios.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU16, NonZeroUsize},
    time::{Duration, Instant},
};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, MetadataGeneration, OutcomeStamp, TopicName,
};

use crate::{DriverLimits, SubmitError};

use super::{RouteFact, RouteKind, identity::DriverIdentity};

#[test]
fn foreign_driver_rejection_returns_the_exact_token() {
    let (issuing, _issuing_reactor) = super::Driver::builder()
        .broker(address(9092))
        .build_reactor()
        .unwrap_or_else(|error| panic!("issuing driver: {error}"));
    let (foreign, _foreign_reactor) = super::Driver::builder()
        .broker(address(9093))
        .build_reactor()
        .unwrap_or_else(|error| panic!("foreign driver: {error}"));
    let token = broker_token(issuing.identity, 7, 9);

    let Err(rejection) =
        foreign.topic_view_after_failure(topic(), token, Instant::now() + Duration::from_secs(1))
    else {
        panic!("foreign causal topic view must be rejected");
    };

    assert!(matches!(rejection.reason(), SubmitError::ForeignDriver));
    let (reason, recovered) = rejection.into_parts();
    assert!(matches!(reason, SubmitError::ForeignDriver));
    assert_eq!(recovered.kind(), RouteKind::Broker);
    assert!(issuing.invalidate(recovered).is_ok());
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
            .invalidate(broker_token(driver.identity, 7, 9))
            .is_ok()
    );

    let Err(rejection) = driver.topic_view_after_failure(
        topic(),
        broker_token(driver.identity, 7, 10),
        Instant::now() + Duration::from_secs(1),
    ) else {
        panic!("causal topic view must exceed mailbox capacity");
    };

    assert!(matches!(rejection.reason(), SubmitError::Full));
    let (reason, recovered) = rejection.into_parts();
    assert!(matches!(reason, SubmitError::Full));
    assert_eq!(recovered.kind(), RouteKind::Broker);
}

fn broker_token(
    driver: DriverIdentity,
    raw_generation: u64,
    raw_outcome: u64,
) -> super::RouteFailureToken {
    let broker_id = BrokerId::new(1).unwrap_or_else(|error| panic!("valid broker ID: {error}"));
    let directory = BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(raw_generation),
        [BrokerDirectoryEntry::new(
            broker_id,
            BrokerEndpoint::new(
                HostName::new("broker.test")
                    .unwrap_or_else(|error| panic!("valid broker host: {error}")),
                NonZeroU16::new(9092).unwrap_or_else(|| panic!("test port must be nonzero")),
            ),
        )],
        BrokerDirectoryLimits::new(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("valid broker directory: {error}"));
    RouteFact::Broker(
        directory
            .route_to(broker_id)
            .unwrap_or_else(|| panic!("directory must issue broker route")),
    )
    .observe(driver, OutcomeStamp::from_raw(raw_outcome))
}

fn topic() -> TopicName {
    TopicName::new("orders").unwrap_or_else(|error| panic!("valid topic: {error}"))
}

const fn address(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}
