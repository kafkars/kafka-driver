//! Public bounded operational-snapshot scenario over the embedded host.

use std::{net::TcpListener, time::Duration};

use kafka_driver::{CallFailure, Delivery, Driver, Reactor, RequestError, TurnOutcome};
use kafka_wire::ApiVersionsRequest;

#[test]
fn snapshot_reports_terminal_public_call_outcomes_and_stage_boundaries() {
    // Given: a direct seed is still opening when one public call is admitted.
    let (driver, mut reactor, _listener) = opening_reactor();
    let call = driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("admit observed request: {error}"));
    let snapshot = driver
        .snapshot()
        .unwrap_or_else(|error| panic!("admit snapshot command: {error}"));

    // When: FIFO command processing rejects the call before preparation.
    let turn = reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("interpret request and snapshot: {error}"));
    let failure = call
        .wait()
        .unwrap_or_else(|error| panic!("observe request completion: {error}"));
    let snapshot = snapshot
        .wait()
        .unwrap_or_else(|error| panic!("observe snapshot completion: {error}"))
        .unwrap_or_else(|error| panic!("snapshot rejected: {error}"));

    // Then: outcomes and only the lifecycle stages actually crossed are counted.
    assert!(matches!(turn, TurnOutcome::Progress { commands: 2, .. }));
    assert_eq!(
        failure,
        Err(RequestError::Rejected {
            failure: CallFailure::NotReady,
            delivery: Delivery::NotSent,
        })
    );
    assert_eq!(snapshot.calls().admitted(), 1);
    assert_eq!(snapshot.calls().failed(), 1);
    assert_eq!(snapshot.calls().succeeded(), 0);
    assert_eq!(snapshot.calls().receiver_abandoned(), 0);
    assert_eq!(snapshot.calls().not_sent(), 1);
    assert_eq!(snapshot.calls().possibly_sent(), 0);
    assert_eq!(snapshot.latency().mailbox().samples(), 1);
    assert_eq!(snapshot.latency().routing().samples(), 1);
    assert_eq!(snapshot.latency().preparation().samples(), 0);
    assert_eq!(snapshot.latency().writer_admission().samples(), 0);
    assert_eq!(snapshot.latency().in_flight().samples(), 0);
    assert_eq!(snapshot.latency().end_to_end().samples(), 1);
    assert_eq!(snapshot.latency().deadline_lateness().samples(), 0);
    assert_eq!(snapshot.mailbox().queued_work(), 0);
    assert_eq!(snapshot.mailbox().queued_control(), 0);
    assert!(snapshot.seed().is_some());
    assert!(snapshot.metadata_generation().is_none());
    assert!(snapshot.lanes().is_empty());
}

#[test]
fn snapshot_counts_a_terminal_value_discarded_after_receiver_abandonment() {
    // Given: the caller abandons an admitted call before the reactor settles it.
    let (driver, mut reactor, _listener) = opening_reactor();
    let call = driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("admit abandoned request: {error}"));
    drop(call);
    let snapshot = driver
        .snapshot()
        .unwrap_or_else(|error| panic!("admit abandonment snapshot: {error}"));

    // When: the reactor rejects that call while its receiver is gone.
    reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("settle abandoned request: {error}"));
    let snapshot = snapshot
        .wait()
        .unwrap_or_else(|error| panic!("observe abandonment snapshot: {error}"))
        .unwrap_or_else(|error| panic!("abandonment snapshot rejected: {error}"));

    // Then: terminal outcome and receiver disposition remain separate facts.
    assert_eq!(snapshot.calls().admitted(), 1);
    assert_eq!(snapshot.calls().failed(), 1);
    assert_eq!(snapshot.calls().receiver_abandoned(), 1);
    assert_eq!(snapshot.calls().not_sent(), 1);
    assert_eq!(snapshot.latency().end_to_end().samples(), 1);
}

#[test]
fn snapshot_reports_deadline_lateness_without_claiming_unreached_stages() {
    // Given: a public call whose absolute deadline is already due at admission.
    let (driver, mut reactor, _listener) = opening_reactor();
    let call = driver
        .call(ApiVersionsRequest::default(), Duration::ZERO)
        .unwrap_or_else(|error| panic!("admit immediately expired request: {error}"));
    let snapshot = driver
        .snapshot()
        .unwrap_or_else(|error| panic!("admit deadline snapshot: {error}"));

    // When: the reactor interprets that deadline before semantic routing.
    reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("settle immediately expired request: {error}"));
    assert_eq!(
        call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        }))
    );
    let snapshot = snapshot
        .wait()
        .unwrap_or_else(|error| panic!("observe deadline snapshot: {error}"))
        .unwrap_or_else(|error| panic!("deadline snapshot rejected: {error}"));

    // Then: one deadline and lateness sample exist, but no route stage is invented.
    assert_eq!(snapshot.failures().deadline(), 1);
    assert_eq!(snapshot.calls().not_sent(), 1);
    assert_eq!(snapshot.latency().mailbox().samples(), 1);
    assert_eq!(snapshot.latency().routing().samples(), 0);
    assert_eq!(snapshot.latency().deadline_lateness().samples(), 1);
    assert_eq!(snapshot.latency().end_to_end().samples(), 1);
}

fn opening_reactor() -> (Driver, Reactor, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind snapshot broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read snapshot broker address: {error}"));
    let (driver, reactor) = Driver::builder()
        .broker(address)
        .build_reactor()
        .unwrap_or_else(|error| panic!("build direct snapshot reactor: {error}"));
    (driver, reactor, listener)
}
