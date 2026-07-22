//! Public embedded-host scenario for generated Kafka RPC over direct rustls I/O.

#![cfg(feature = "tls-rustls")]

#[path = "support/tls_broker.rs"]
mod tls_broker;

use std::{
    sync::mpsc::{Receiver, TryRecvError},
    time::Duration,
};

use kafka_driver::{Driver, Reactor, TurnOutcome};
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
    drive_once(&mut reactor);
    owner
        .join()
        .unwrap_or_else(|_| panic!("TLS broker fixture must finish cleanly"));

    // Then
    assert_eq!(call.wait(), Ok(Ok(response)));
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
