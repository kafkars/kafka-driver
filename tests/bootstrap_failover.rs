//! Public real-loop scenarios for bootstrap failover after one resolved endpoint fails.

mod support;

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    num::NonZeroU16,
    time::Duration,
};

use kafka_driver::{
    BootstrapLimits, BootstrapSet, BrokerEndpoint, ConnectionCloseReason, ConnectionPhase, Driver,
    HostName, NegotiationFailure, Reactor,
};

use support::complete_negotiation;

#[test]
fn reset_first_bootstrap_endpoint_rotates_after_complete_dial_failure() {
    // Given
    let reset = listener();
    let available = listener();
    let available_port = local_port(&available);
    let (_driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap(local_port(&reset), available_port))
        .build_reactor()
        .unwrap_or_else(|error| panic!("build bootstrap reactor: {error}"));

    // When
    let reset_peer = accept_after_driving(&reset, &mut reactor);
    drop(reset_peer);
    let mut peer = accept_after_driving(&available, &mut reactor);
    complete_negotiation(&mut peer, &mut reactor);

    // Then
    let diagnostics = format!("{reactor:?}");
    assert!(
        diagnostics.contains("connection: Some(Ready"),
        "second bootstrap endpoint must become ready: {diagnostics}"
    );
}

#[test]
fn malformed_negotiation_at_first_bootstrap_endpoint_rotates_to_second() {
    // Given
    let malformed = listener();
    let available = listener();
    let (driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap(local_port(&malformed), local_port(&available)))
        .build_reactor()
        .unwrap_or_else(|error| panic!("build bootstrap reactor: {error}"));

    // When
    let mut malformed_peer = accept_after_driving(&malformed, &mut reactor);
    fail_negotiation(&mut malformed_peer, &mut reactor);
    let mut available_peer = accept_after_driving(&available, &mut reactor);
    complete_negotiation(&mut available_peer, &mut reactor);
    let snapshot = snapshot(&driver, &mut reactor);

    // Then
    let seed = snapshot
        .seed()
        .unwrap_or_else(|| panic!("successful failover must retain seed ownership"));
    assert_eq!(seed.connection_phase(), ConnectionPhase::Ready);
    assert_eq!(
        seed.last_close_reason(),
        Some(ConnectionCloseReason::NegotiationFailed(
            NegotiationFailure::Malformed
        ))
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

fn snapshot(driver: &Driver, reactor: &mut Reactor) -> kafka_driver::DriverSnapshot {
    let snapshot = driver
        .snapshot()
        .unwrap_or_else(|error| panic!("admit failover snapshot: {error}"));
    reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("interpret failover snapshot: {error}"));
    snapshot
        .wait()
        .unwrap_or_else(|error| panic!("observe failover snapshot: {error}"))
        .unwrap_or_else(|error| panic!("build failover snapshot: {error}"))
}

fn fail_negotiation(peer: &mut TcpStream, reactor: &mut Reactor) {
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("bound malformed broker read: {error}"));
    drive(reactor);
    drive(reactor);
    let mut prefix = [0; size_of::<i32>()];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read negotiation frame length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("validate negotiation frame length: {error}"));
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read negotiation frame body: {error}"));
    peer.write_all(&0_i32.to_be_bytes())
        .unwrap_or_else(|error| panic!("write malformed negotiation response: {error}"));
    drive(reactor);
}

fn drive(reactor: &mut Reactor) {
    reactor
        .turn(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("drive malformed negotiation: {error}"));
}
