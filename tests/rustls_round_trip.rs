//! Public embedded-host scenario for generated Kafka RPC over direct rustls I/O.

#![cfg(feature = "tls-rustls")]

#[path = "support/tls_broker.rs"]
mod tls_broker;

use std::{
    sync::mpsc::{Receiver, TryRecvError},
    time::Duration,
};

use kafka_driver::{
    Call, CallFailure, CompletionError, ConnectionCloseReason, Delivery, Driver, Reactor,
    RequestError, TransportFailure, TurnOutcome,
};
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};

use tls_broker::{BrokerStep, TlsBroker};

#[test]
fn generated_call_round_trips_through_the_public_rustls_host() {
    // Given
    let broker = TlsBroker::bind();
    let address = broker.address();
    let tls = broker.client_config();
    let (steps, owner) = broker.spawn();
    let Ok((driver, mut reactor)) = Driver::builder()
        .rustls_broker(address, tls)
        .build_reactor()
    else {
        panic!("build configured rustls reactor");
    };
    drive_until(&mut reactor, &steps, BrokerStep::NegotiationResponded);
    drive_once(&mut reactor);
    let response = ApiVersionsResponse::default();
    let Ok(call) = driver.call(ApiVersionsRequest::default(), Duration::from_secs(1)) else {
        panic!("admit generated TLS call command");
    };

    // When
    let admitted = reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("admit TLS call: {error}"));
    assert!(matches!(
        admitted,
        TurnOutcome::Progress { commands: 1, .. }
    ));
    drive_until(&mut reactor, &steps, BrokerStep::CallResponded);
    let result = drive_call_until_ready(&mut reactor, &call);
    owner
        .join()
        .unwrap_or_else(|_| panic!("TLS broker fixture must finish cleanly"));

    // Then
    assert_eq!(result, Ok(Ok(response)));
}

#[test]
fn valid_tls_response_precedes_a_malformed_trailing_frame() {
    let broker = TlsBroker::bind();
    let address = broker.address();
    let tls = broker.client_config();
    let (steps, owner) = broker.spawn_malformed_after_call();
    let Ok((driver, mut reactor)) = Driver::builder()
        .rustls_broker(address, tls)
        .build_reactor()
    else {
        panic!("build configured rustls reactor");
    };
    drive_until(&mut reactor, &steps, BrokerStep::NegotiationResponded);
    drive_once(&mut reactor);
    let response = ApiVersionsResponse::default();
    let Ok(call) = driver.call(ApiVersionsRequest::default(), Duration::from_secs(1)) else {
        panic!("admit generated TLS call command");
    };
    let admitted = reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("admit TLS call: {error}"));
    assert!(matches!(
        admitted,
        TurnOutcome::Progress { commands: 1, .. }
    ));

    drive_until(
        &mut reactor,
        &steps,
        BrokerStep::CallRespondedBeforeMalformedFrame,
    );
    let result = drive_call_until_ready(&mut reactor, &call);
    owner
        .join()
        .unwrap_or_else(|_| panic!("TLS broker fixture must finish cleanly"));

    assert_eq!(result, Ok(Ok(response)));
}

#[test]
fn two_correlated_responses_complete_before_terminal_tls_truncation() {
    let broker = TlsBroker::bind();
    let address = broker.address();
    let tls = broker.client_config();
    let (steps, owner) = broker.spawn_two_calls_before_truncation();
    let Ok((driver, mut reactor)) = Driver::builder()
        .rustls_broker(address, tls)
        .build_reactor()
    else {
        panic!("build two-call rustls reactor");
    };
    drive_until(&mut reactor, &steps, BrokerStep::NegotiationResponded);
    let first = driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("admit first batched TLS call: {error}"));
    let second = driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("admit second batched TLS call: {error}"));

    let admitted = reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("admit batched TLS calls: {error}"));
    assert!(matches!(
        admitted,
        TurnOutcome::Progress { commands: 2, .. }
    ));
    drive_until(
        &mut reactor,
        &steps,
        BrokerStep::CallsRespondedBeforeTruncation,
    );
    let first = drive_call_until_ready(&mut reactor, &first);
    let second = drive_call_until_ready(&mut reactor, &second);
    assert_eq!(first, Ok(Ok(ApiVersionsResponse::default())));
    assert_eq!(second, Ok(Ok(ApiVersionsResponse::default())));

    for _ in 0..4 {
        drive_once(&mut reactor);
    }
    let probe = driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("admit post-truncation probe: {error}"));
    drive_once(&mut reactor);
    let probe = drive_call_until_ready(&mut reactor, &probe);
    owner
        .join()
        .unwrap_or_else(|_| panic!("two-call TLS broker must finish cleanly"));

    assert_eq!(
        probe,
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::Closed,
            delivery: Delivery::NotSent,
        }))
    );
}

#[test]
fn loopback_ip_identity_round_trips_without_dns_sni() {
    let broker = TlsBroker::bind_loopback_ip();
    let address = broker.address();
    let tls = broker.client_config_for_ip();
    let (steps, owner) = broker.spawn_expecting_no_sni();
    let Ok((driver, mut reactor)) = Driver::builder()
        .rustls_broker(address, tls)
        .build_reactor()
    else {
        panic!("build IP-identity rustls reactor");
    };
    drive_until(
        &mut reactor,
        &steps,
        BrokerStep::NegotiationRespondedWithoutSni,
    );
    let Ok(call) = driver.call(ApiVersionsRequest::default(), Duration::from_secs(1)) else {
        panic!("admit IP-identity TLS call");
    };

    drive_once(&mut reactor);
    drive_until(&mut reactor, &steps, BrokerStep::CallResponded);
    let result = drive_call_until_ready(&mut reactor, &call);
    owner
        .join()
        .unwrap_or_else(|_| panic!("IP-identity TLS broker must finish cleanly"));

    assert_eq!(result, Ok(Ok(ApiVersionsResponse::default())));
}

#[test]
fn wrong_logical_name_fails_establishment_before_kafka_admission() {
    let broker = TlsBroker::bind();
    let address = broker.address();
    let tls = broker.client_config_for("wrong.invalid");
    let (steps, owner) = broker.spawn_observing_identity_rejection();
    let Ok((driver, mut reactor)) = Driver::builder()
        .rustls_broker(address, tls)
        .build_reactor()
    else {
        panic!("build wrong-name rustls reactor");
    };
    let Ok(call) = driver.call(ApiVersionsRequest::default(), Duration::from_secs(1)) else {
        panic!("retain call behind TLS identity establishment");
    };

    drive_until(
        &mut reactor,
        &steps,
        BrokerStep::TlsHandshakeRejectedBeforeKafka,
    );
    let result = drive_call_until_ready(&mut reactor, &call);
    owner
        .join()
        .unwrap_or_else(|_| panic!("wrong-name TLS broker must finish cleanly"));

    assert_eq!(
        result,
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::ConnectionClosed {
                reason: ConnectionCloseReason::OpenFailed(TransportFailure::Other),
            },
            delivery: Delivery::NotSent,
        }))
    );
}

#[test]
fn unauthenticated_tcp_truncation_resets_a_possibly_sent_call() {
    let broker = TlsBroker::bind();
    let address = broker.address();
    let tls = broker.client_config();
    let (steps, owner) = broker.spawn_truncating_after_call();
    let Ok((driver, mut reactor)) = Driver::builder()
        .rustls_broker(address, tls)
        .build_reactor()
    else {
        panic!("build truncating rustls reactor");
    };
    drive_until(&mut reactor, &steps, BrokerStep::NegotiationResponded);
    let Ok(call) = driver.call(ApiVersionsRequest::default(), Duration::from_secs(1)) else {
        panic!("admit TLS call before raw truncation");
    };

    drive_once(&mut reactor);
    drive_until(&mut reactor, &steps, BrokerStep::CallReadBeforeTruncation);
    let result = drive_call_until_ready(&mut reactor, &call);
    owner
        .join()
        .unwrap_or_else(|_| panic!("truncating TLS broker must finish cleanly"));

    assert_eq!(
        result,
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::ConnectionClosed {
                reason: ConnectionCloseReason::TransportLost(TransportFailure::Reset),
            },
            delivery: Delivery::PossiblySent,
        }))
    );
}

fn drive_call_until_ready<T>(reactor: &mut Reactor, call: &Call<T>) -> Result<T, CompletionError> {
    for _ in 0..64 {
        if let Some(result) = call.try_result() {
            return result;
        }
        drive_once(reactor);
    }
    panic!("TLS call remained pending");
}

fn drive_until(reactor: &mut Reactor, steps: &Receiver<BrokerStep>, expected: BrokerStep) {
    for _ in 0..64 {
        match steps.try_recv() {
            Ok(observed) => {
                assert_eq!(observed, expected);
                return;
            }
            Err(TryRecvError::Disconnected) => {
                panic!("TLS broker fixture stopped early with reactor {reactor:?}")
            }
            Err(TryRecvError::Empty) => drive_once(reactor),
        }
    }
    panic!("TLS broker did not reach {expected:?}");
}

fn drive_once(reactor: &mut Reactor) {
    let outcome = reactor
        .turn(Duration::from_millis(100))
        .unwrap_or_else(|error| panic!("drive TLS reactor turn: {error}"));
    assert!(!matches!(outcome, TurnOutcome::Shutdown { .. }));
}
