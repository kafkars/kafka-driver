//! Public scenarios for client-owned request submission policy.

use std::{
    net::TcpListener,
    time::{Duration, Instant},
};

use kafka_driver::{
    ApiVersion, CallFailure, Delivery, Driver, RequestError, RequestOptions, Route, SubmitError,
    TrafficClass, TurnOutcome,
};
use kafka_wire::{API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest};

#[test]
fn original_absolute_deadline_expires_before_routing_or_io() {
    // Given: client work consumed the request budget before driver submission.
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind unopened broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read broker address: {error}"));
    let (driver, mut reactor) = Driver::builder()
        .broker(address)
        .build_reactor()
        .unwrap_or_else(|error| panic!("build direct reactor: {error}"));
    let deadline = Instant::now()
        .checked_sub(Duration::from_secs(1))
        .unwrap_or_else(|| panic!("test instant must have one second of history"));
    let options = RequestOptions::new(deadline).with_traffic_class(TrafficClass::Control);
    let options = options
        .with_minimum_version(ApiVersion::new(3))
        .with_maximum_version(ApiVersion::new(12));

    // When: the driver admits the request with its original deadline.
    let call = driver
        .request_with(Route::AnyBroker, ApiVersionsRequest::default(), options)
        .unwrap_or_else(|error| panic!("admit expired request: {error}"));
    reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("settle expired request: {error}"));

    // Then: expiry wins before connection readiness can affect the outcome.
    assert_eq!(
        call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        }))
    );
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Control);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(3)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(12)));
}

#[test]
fn reversed_version_bounds_never_enter_public_request_admission() {
    // Given: contradictory bounds for both public options-based entry points.
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind unopened broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read broker address: {error}"));
    let (driver, mut reactor) = Driver::builder()
        .broker(address)
        .build_reactor()
        .unwrap_or_else(|error| panic!("build direct reactor: {error}"));
    let options = RequestOptions::new(Instant::now())
        .with_minimum_version(ApiVersion::new(12))
        .with_maximum_version(ApiVersion::new(9));

    // When: ordinary and route-tracked calls attempt admission.
    let ordinary = driver.request_with(Route::Controller, ApiVersionsRequest::default(), options);
    let tracked =
        driver.request_tracked_with(Route::Controller, ApiVersionsRequest::default(), options);

    // Then: both fail synchronously and no request command reaches the reactor.
    for rejection in [ordinary.map(|_| ()), tracked.map(|_| ())] {
        assert!(matches!(
            rejection,
            Err(SubmitError::VersionBoundsInvalid {
                api_key,
                minimum,
                maximum,
            }) if api_key == API_VERSIONS_API_DESCRIPTOR.api_key
                && minimum == ApiVersion::new(12)
                && maximum == ApiVersion::new(9)
        ));
    }
    let turn = reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("observe empty request mailbox: {error}"));
    assert!(matches!(
        turn,
        TurnOutcome::Idle | TurnOutcome::Progress { commands: 0, .. }
    ));
}
