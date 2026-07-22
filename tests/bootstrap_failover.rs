//! Public real-loop scenario for bootstrap failover after a resolved socket refuses.

mod support;

use std::{
    net::{TcpListener, TcpStream},
    num::NonZeroU16,
    time::Duration,
};

use kafka_driver::{BootstrapLimits, BootstrapSet, BrokerEndpoint, Driver, HostName, Reactor};

use support::complete_negotiation;

#[test]
fn refused_first_bootstrap_endpoint_rotates_after_complete_dial_failure() {
    // Given
    let refused = listener();
    let refused_port = local_port(&refused);
    let available = listener();
    let available_port = local_port(&available);
    drop(refused);
    let (_driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap(refused_port, available_port))
        .build_reactor()
        .unwrap_or_else(|error| panic!("build bootstrap reactor: {error}"));

    // When
    let mut peer = accept_after_driving(&available, &mut reactor);
    complete_negotiation(&mut peer, &mut reactor);

    // Then
    let diagnostics = format!("{reactor:?}");
    assert!(
        diagnostics.contains("connection: Some(Ready"),
        "second bootstrap endpoint must become ready: {diagnostics}"
    );
}

fn accept_after_driving(listener: &TcpListener, reactor: &mut Reactor) -> TcpStream {
    listener
        .set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make bootstrap listener nonblocking: {error}"));
    for _ in 0..32 {
        reactor
            .turn(Duration::from_millis(100))
            .unwrap_or_else(|error| panic!("drive bootstrap failover: {error}"));
        match listener.accept() {
            Ok((peer, _)) => {
                peer.set_nonblocking(false)
                    .unwrap_or_else(|error| panic!("make bootstrap peer blocking: {error}"));
                return peer;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("accept bootstrap connection: {error}"),
        }
    }
    panic!("bootstrap failover did not reach the second endpoint: {reactor:?}");
}

fn bootstrap(first_port: u16, second_port: u16) -> BootstrapSet {
    let host = HostName::new("127.0.0.1")
        .unwrap_or_else(|error| panic!("numeric bootstrap host: {error}"));
    BootstrapSet::try_from_iter(
        [
            BrokerEndpoint::new(host.clone(), port(first_port)),
            BrokerEndpoint::new(host, port(second_port)),
        ],
        BootstrapLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid bootstrap membership: {error}"))
}

fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap_or_else(|error| panic!("bind loopback broker: {error}"))
}

fn local_port(listener: &TcpListener) -> u16 {
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback address: {error}"))
        .port()
}

fn port(raw: u16) -> NonZeroU16 {
    NonZeroU16::new(raw).unwrap_or_else(|| panic!("listener port is nonzero"))
}
