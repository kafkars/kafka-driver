//! Public startup failures returned through the bounded dedicated-host handshake.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use kafka_driver::{Driver, DriverBuildError};

#[test]
fn targetless_dedicated_builder_preserves_the_exact_startup_error() {
    let result = Driver::builder().spawn();

    assert!(matches!(result, Err(DriverBuildError::MissingTarget)));
}

#[test]
fn dedicated_builder_preserves_client_id_validation_before_reactor_construction() {
    let result = Driver::builder()
        .broker(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9092))
        .client_id("x".repeat(i16::MAX as usize + 1))
        .spawn();

    assert!(matches!(
        result,
        Err(DriverBuildError::ClientIdTooLong { actual, limit })
            if actual == i16::MAX as usize + 1 && limit == i16::MAX as usize
    ));
}
