//! Public real-loop scenario for off-shard DNS bootstrap into broker ownership.

mod support;

use std::{
    io::Read,
    net::{TcpListener, TcpStream},
    num::NonZeroU16,
    time::Duration,
};

use kafka_driver::{BootstrapLimits, BootstrapSet, BrokerEndpoint, Driver, HostName, TurnOutcome};
use kafka_wire::ApiVersionsRequest;

use support::complete_negotiation;

#[test]
fn numeric_bootstrap_resolves_off_shard_and_installs_a_ready_broker() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback broker address: {error}"));
    let bootstrap = bootstrap(address.port());
    let (driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap)
        .build_reactor()
        .unwrap_or_else(|error| panic!("build bootstrap reactor: {error}"));

    let outcome = reactor
        .turn(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("drive DNS bootstrap: {error}"));
    assert!(matches!(outcome, TurnOutcome::Progress { commands: 0, .. }));
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept resolved broker connection: {error}"));
    complete_negotiation(&mut peer, &mut reactor);

    let call = driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("admit generated request: {error}"));
    let outcome = reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("submit request to bootstrapped broker: {error}"));
    let write = reactor
        .turn(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("write request to bootstrapped broker: {error}"));

    assert!(matches!(outcome, TurnOutcome::Progress { commands: 1, .. }));
    assert!(matches!(write, TurnOutcome::Progress { commands: 0, .. }));
    read_frame(&mut peer);
    drop(call);
}

fn bootstrap(port: u16) -> BootstrapSet {
    let host = HostName::new("127.0.0.1")
        .unwrap_or_else(|error| panic!("numeric bootstrap host: {error}"));
    let port = NonZeroU16::new(port).unwrap_or_else(|| panic!("listener port must be nonzero"));
    BootstrapSet::try_from_iter(
        [BrokerEndpoint::new(host, port)],
        BootstrapLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid bootstrap membership: {error}"))
}

fn read_frame(peer: &mut TcpStream) {
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("bound broker read: {error}"));
    let mut prefix = [0; size_of::<i32>()];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read request frame length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("validate request frame length: {error}"));
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read request frame body: {error}"));
    assert!(!body.is_empty(), "generated request body must not be empty");
}
