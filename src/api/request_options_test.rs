//! Scenarios proving option validation precedes public call identity ownership.

use std::{sync::Arc, time::Instant};

use kafka_driver_core::CallId;
use kafka_wire::ApiVersionsRequest;
use kafka_wire_core::ApiVersion;

use crate::{
    DriverLimits, RequestOptions, Route, SubmitError, TurnOutcome, observation::Observation,
    reactor::Reactor,
};

use super::{CallIds, Driver, DriverIdentity};

#[test]
fn reversed_version_bounds_do_not_allocate_a_call_identity() {
    // Given: a public driver whose next call identity is one.
    let call_ids = Arc::new(CallIds::new());
    let observation = Arc::new(Observation::default());
    let (commands, shutdown, mut reactor) = Reactor::new_legacy_test(
        &DriverLimits::default(),
        Arc::clone(&call_ids),
        Arc::clone(&observation),
    )
    .unwrap_or_else(|error| panic!("build targetless test reactor: {error}"));
    let identity =
        DriverIdentity::allocate().unwrap_or_else(|| panic!("allocate test driver identity"));
    let driver = Driver::new(
        commands,
        shutdown,
        call_ids.clone(),
        observation,
        identity,
        0,
    );
    let options = RequestOptions::new(Instant::now())
        .with_minimum_version(ApiVersion::new(12))
        .with_maximum_version(ApiVersion::new(9));

    // When: the caller submits contradictory version bounds.
    let rejection = driver.request_with(Route::Controller, ApiVersionsRequest::default(), options);

    // Then: rejection precedes both identity allocation and mailbox ownership.
    assert!(matches!(
        rejection,
        Err(SubmitError::VersionBoundsInvalid { .. })
    ));
    assert_eq!(call_ids.allocate(), Some(CallId::from_raw(1)));
    assert!(matches!(
        reactor.turn(std::time::Duration::ZERO),
        Ok(TurnOutcome::Idle)
    ));
}

#[test]
fn route_failure_rejection_is_explicit_and_disabled_by_default() {
    let deadline = Instant::now();
    assert!(!RequestOptions::new(deadline).rejects_after_route_failure());
    assert!(
        RequestOptions::new(deadline)
            .with_route_failure_rejection()
            .rejects_after_route_failure()
    );
}
