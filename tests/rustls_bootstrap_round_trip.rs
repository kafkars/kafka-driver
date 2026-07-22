//! Public embedded-host scenario for TLS bootstrap with endpoint-derived identity.

#![cfg(feature = "tls-rustls")]

#[path = "support/tls_broker.rs"]
mod tls_broker;

use std::{
    num::NonZeroU16,
    sync::mpsc::{Receiver, TryRecvError},
    time::Duration,
};

use kafka_driver::{
    BootstrapLimits, BootstrapSet, BrokerEndpoint, Driver, HostName, Reactor, TurnOutcome,
};
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};

use tls_broker::{BrokerStep, TlsBroker};

#[test]
fn logical_bootstrap_host_supplies_its_own_tls_server_identity() {
    // Given
    let broker = TlsBroker::bind();
    let bootstrap = bootstrap(broker.address().port());
    let policy = broker.client_policy();
    let (steps, owner) = broker.spawn();
    let Ok((driver, mut reactor)) = Driver::builder()
        .rustls_bootstrap(bootstrap, policy)
        .build_reactor()
    else {
        panic!("build TLS bootstrap reactor");
    };
    drive_until(&mut reactor, &steps, BrokerStep::NegotiationResponded);
    drive_once(&mut reactor);
    let Ok(call) = driver.call(ApiVersionsRequest::default(), Duration::from_secs(1)) else {
        panic!("admit generated TLS call command");
    };

    // When
    drive_once(&mut reactor);
    drive_until(&mut reactor, &steps, BrokerStep::CallResponded);
    drive_once(&mut reactor);
    owner
        .join()
        .unwrap_or_else(|_| panic!("TLS broker fixture must finish cleanly"));

    // Then
    assert_eq!(call.wait(), Ok(Ok(ApiVersionsResponse::default())));
}

fn bootstrap(port: u16) -> BootstrapSet {
    let host = HostName::new("localhost")
        .unwrap_or_else(|error| panic!("construct TLS bootstrap host: {error}"));
    let port = NonZeroU16::new(port).unwrap_or_else(|| panic!("listener port must be nonzero"));
    BootstrapSet::try_from_iter(
        [BrokerEndpoint::new(host, port)],
        BootstrapLimits::default(),
    )
    .unwrap_or_else(|error| panic!("construct TLS bootstrap set: {error}"))
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
        .unwrap_or_else(|error| panic!("drive TLS bootstrap reactor: {error}"));
    assert!(!matches!(outcome, TurnOutcome::Shutdown { .. }));
}
