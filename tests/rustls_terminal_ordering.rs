//! Live TLS response-prefix ordering across Bornera recovery generations.

#![cfg(feature = "tls-rustls")]

#[path = "support/tls_broker.rs"]
mod tls_broker;

use std::{
    sync::mpsc::{Receiver, TryRecvError},
    time::Duration,
};

use kafka_driver::{
    BrokerState, Call, CallFailure, CompletionError, ConnectionCloseReason, ConnectionPhase,
    Delivery, Driver, DriverSnapshot, Reactor, RequestError, TurnOutcome,
};
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};

use tls_broker::{TerminalScript, TerminalStep, TlsBroker};

type ApiCall = Call<Result<ApiVersionsResponse, RequestError>>;
type ApiResult = Result<Result<ApiVersionsResponse, RequestError>, CompletionError>;

#[test]
fn valid_response_precedes_tls_close_notify_and_eof() {
    assert_terminal_ordering(TerminalScript::CloseNotifyAfterOne);
}

#[test]
fn valid_response_precedes_tls_truncation() {
    assert_terminal_ordering(TerminalScript::TruncateAfterOne);
}

#[test]
fn two_coalesced_responses_precede_tls_truncation() {
    assert_terminal_ordering(TerminalScript::TruncateAfterTwo);
}

#[test]
fn complete_response_precedes_partial_trailing_frame_failure() {
    assert_terminal_ordering(TerminalScript::PartialAfterOne);
}

fn assert_terminal_ordering(script: TerminalScript) {
    let broker = TlsBroker::bind();
    let address = broker.address();
    let tls = broker.client_config();
    let (steps, release, owner) = broker.spawn_terminal_ordering(script);
    let (driver, mut reactor) = Driver::builder()
        .rustls_broker(address, tls)
        .build_reactor()
        .unwrap_or_else(|error| panic!("build TLS terminal-ordering reactor: {error}"));
    let calls = (0..script.generation_one_calls())
        .map(|_| admit_call(&driver))
        .collect::<Vec<_>>();

    drive_until(&mut reactor, &steps, TerminalStep::GenerationOneClosed);
    for call in calls.iter().take(script.complete_responses()) {
        assert_eq!(
            drive_call(&mut reactor, call),
            Ok(Ok(ApiVersionsResponse::default()))
        );
    }
    for call in calls.iter().skip(script.complete_responses()) {
        assert_transport_failure(&drive_call(&mut reactor, call));
    }
    for call in &calls {
        assert_eq!(call.try_result(), Some(Err(CompletionError::Consumed)));
    }

    drive_until(&mut reactor, &steps, TerminalStep::GenerationTwoNegotiated);
    let probe = admit_call(&driver);
    drive_until(&mut reactor, &steps, TerminalStep::ProbeResponded);
    assert_eq!(
        drive_call(&mut reactor, &probe),
        Ok(Ok(ApiVersionsResponse::default()))
    );
    assert_terminal_snapshot(
        &current_snapshot(&driver, &mut reactor),
        script.generation_one_calls(),
        script.complete_responses(),
    );

    release
        .send(())
        .unwrap_or_else(|error| panic!("release TLS recovery fixture: {error}"));
    owner
        .join()
        .unwrap_or_else(|_| panic!("TLS terminal-ordering fixture must finish cleanly"));
}

fn admit_call(driver: &Driver) -> ApiCall {
    driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("admit TLS terminal-ordering call: {error}"))
}

fn assert_transport_failure(result: &ApiResult) {
    assert!(matches!(
        result,
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::ConnectionClosed {
                reason: ConnectionCloseReason::TransportLost(_),
            },
            delivery: Delivery::PossiblySent,
        }))
    ));
}

fn assert_terminal_snapshot(snapshot: &DriverSnapshot, calls: usize, complete: usize) {
    let calls = u64::try_from(calls).unwrap_or_else(|error| panic!("bound call count: {error}"));
    let complete =
        u64::try_from(complete).unwrap_or_else(|error| panic!("bound completion count: {error}"));
    let seed = snapshot
        .seed()
        .unwrap_or_else(|| panic!("recovered TLS seed must remain observable"));
    assert!(matches!(
        seed.broker_state(),
        BrokerState::Available { epoch } if epoch.get() == 2
    ));
    assert_eq!(seed.connection_phase(), ConnectionPhase::Ready);
    assert!(matches!(
        seed.last_close_reason(),
        Some(ConnectionCloseReason::TransportLost(_))
    ));
    assert_eq!(seed.write_queue().queued_frames(), 0);
    assert_eq!(seed.write_queue().retained_bytes(), 0);
    assert_eq!(snapshot.calls().admitted(), calls + 1);
    assert_eq!(snapshot.calls().succeeded(), complete + 1);
    assert_eq!(snapshot.calls().failed(), calls - complete);
    assert_eq!(snapshot.calls().possibly_sent(), calls - complete);
    assert_eq!(snapshot.failures().transport(), calls - complete);
    assert_eq!(snapshot.latency().end_to_end().samples(), calls + 1);
}

fn current_snapshot(driver: &Driver, reactor: &mut Reactor) -> DriverSnapshot {
    let snapshot = driver
        .snapshot()
        .unwrap_or_else(|error| panic!("admit TLS terminal snapshot: {error}"));
    drive_once(reactor);
    snapshot
        .wait()
        .unwrap_or_else(|error| panic!("receive TLS terminal snapshot: {error}"))
        .unwrap_or_else(|error| panic!("build TLS terminal snapshot: {error}"))
}

fn drive_call(reactor: &mut Reactor, call: &ApiCall) -> ApiResult {
    for _ in 0..256 {
        if let Some(result) = call.try_result() {
            return result;
        }
        drive_once(reactor);
    }
    panic!("TLS terminal-ordering call remained pending");
}

fn drive_until(reactor: &mut Reactor, steps: &Receiver<TerminalStep>, expected: TerminalStep) {
    for _ in 0..256 {
        match steps.try_recv() {
            Ok(observed) => {
                assert_eq!(observed, expected);
                return;
            }
            Err(TryRecvError::Disconnected) => {
                panic!("TLS terminal fixture stopped before {expected:?}")
            }
            Err(TryRecvError::Empty) => drive_once(reactor),
        }
    }
    panic!("TLS terminal fixture did not reach {expected:?}");
}

fn drive_once(reactor: &mut Reactor) {
    let outcome = reactor
        .turn(Duration::from_millis(25))
        .unwrap_or_else(|error| panic!("drive TLS terminal-ordering turn: {error}"));
    assert!(!matches!(outcome, TurnOutcome::Shutdown { .. }));
}
