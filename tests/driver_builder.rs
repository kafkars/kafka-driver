//! Public construction scenarios requiring one explicit Kafka target.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use kafka_driver::{Driver, DriverBuildError, SaslConfig};

#[test]
fn targetless_builder_is_rejected_instead_of_creating_an_inert_driver() {
    let result = Driver::builder().build_reactor();

    assert!(matches!(result, Err(DriverBuildError::MissingTarget)));
}

#[test]
fn targetless_sasl_configuration_is_rejected_instead_of_being_discarded() {
    let sasl = SaslConfig::plain("alice", "secret")
        .unwrap_or_else(|error| panic!("construct valid SASL credentials: {error}"));

    let result = Driver::builder().sasl(sasl).build_reactor();

    assert!(matches!(result, Err(DriverBuildError::MissingTarget)));
}

#[test]
fn oversized_client_id_is_rejected_before_reactor_construction() {
    let result = Driver::builder()
        .broker(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9092))
        .client_id("x".repeat(i16::MAX as usize + 1))
        .build_reactor();

    assert!(matches!(
        result,
        Err(DriverBuildError::ClientIdTooLong { actual, limit })
            if actual == i16::MAX as usize + 1 && limit == i16::MAX as usize
    ));
}
