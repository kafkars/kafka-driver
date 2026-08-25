//! Dedicated-host proof for rustls roundtrip, drain, and authenticated shutdown.

#![cfg(feature = "tls-rustls")]

#[path = "support/tls_broker.rs"]
mod tls_broker;

use std::{sync::mpsc::Receiver, time::Duration};

use kafka_driver::Driver;
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};

use tls_broker::{BrokerStep, TlsBroker};

#[test]
fn dedicated_rustls_host_drains_with_close_notify() {
    let broker = TlsBroker::bind();
    let address = broker.address();
    let tls = broker.client_config();
    let (steps, broker_owner) = broker.spawn_observing_close_notify();
    let (driver, host) = Driver::builder()
        .rustls_broker(address, tls)
        .spawn()
        .unwrap_or_else(|error| panic!("spawn dedicated rustls host: {error}"));
    expect_step(&steps, BrokerStep::NegotiationResponded);
    let call = driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("admit dedicated TLS call: {error}"));

    expect_step(&steps, BrokerStep::CallResponded);
    assert_eq!(call.wait(), Ok(Ok(ApiVersionsResponse::default())));
    let shutdown = driver
        .shutdown()
        .unwrap_or_else(|error| panic!("request dedicated TLS drain: {error}"));
    expect_step(&steps, BrokerStep::CloseNotifyObserved);

    assert_eq!(shutdown.wait(), Ok(()));
    assert!(host.join().is_ok());
    broker_owner
        .join()
        .unwrap_or_else(|_| panic!("dedicated TLS broker must finish cleanly"));
}

fn expect_step(steps: &Receiver<BrokerStep>, expected: BrokerStep) {
    let observed = steps
        .recv_timeout(Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("TLS broker did not reach {expected:?}: {error}"));
    assert_eq!(observed, expected);
}
