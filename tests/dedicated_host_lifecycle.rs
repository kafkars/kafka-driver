//! Public lifecycle scenarios for the dedicated reactor thread owner.

use std::net::TcpListener;

use kafka_driver::{Driver, DriverHostError};

#[test]
fn explicit_shutdown_completes_before_the_host_joins_successfully() {
    // Given
    let (driver, host, _listener) = spawn_driver();
    let shutdown = driver
        .shutdown()
        .unwrap_or_else(|error| panic!("admit dedicated shutdown: {error}"));

    // When
    let completion = shutdown.wait();
    let joined = host.join();

    // Then
    assert_eq!(completion, Ok(()));
    assert!(joined.is_ok());
    let completed = driver
        .shutdown()
        .unwrap_or_else(|error| panic!("subscribe to completed shutdown: {error}"));
    assert_eq!(completed.wait(), Ok(()));
}

#[test]
fn dropping_the_last_driver_wakes_and_stops_the_dedicated_host() {
    // Given
    let (driver, host, _listener) = spawn_driver();

    // When
    drop(driver);

    // Then
    assert!(host.join().is_ok());
}

#[test]
fn dropping_join_ownership_does_not_take_shutdown_authority_from_the_driver() {
    // Given
    let (driver, host, _listener) = spawn_driver();
    drop(host);

    // When
    let shutdown = driver
        .shutdown()
        .unwrap_or_else(|error| panic!("admit detached-host shutdown: {error}"));

    // Then
    assert_eq!(shutdown.wait(), Ok(()));
}

#[test]
fn panic_failure_is_sanitized_and_has_no_source_payload() {
    let failure = DriverHostError::Panicked;

    assert_eq!(failure.to_string(), "the dedicated driver host panicked");
    assert!(std::error::Error::source(&failure).is_none());
}

fn spawn_driver() -> (Driver, kafka_driver::DriverHost, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind dedicated lifecycle broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read dedicated lifecycle address: {error}"));
    let (driver, host) = Driver::builder()
        .broker(address)
        .spawn()
        .unwrap_or_else(|error| panic!("spawn dedicated driver host: {error}"));
    (driver, host, listener)
}
