//! Public scenarios for client-owned request submission policy.

use std::{
    net::TcpListener,
    time::{Duration, Instant},
};

use kafka_driver::{
    CallFailure, Delivery, Driver, RequestError, RequestOptions, Route, TrafficClass,
};
use kafka_wire::ApiVersionsRequest;

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
}
