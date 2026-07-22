//! Loopback scenarios for atomic resource open, readiness lookup, and close.

use std::{net::TcpListener, num::NonZeroUsize, time::Duration};

use kafka_driver_core::{ConnectionEpoch, TransportId};

use crate::config::BrokerSecurity;
use crate::reactor::{PollEvent, Poller, tcp::ConnectProgress, transport::TransportLimits};

use super::{ResourceIdentity, transport::TransportResources};

#[test]
fn opened_resource_is_registered_and_found_by_readiness_token() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback listener: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback address: {error}"));
    let mut poller = poller();
    let mut resources = TransportResources::new(
        NonZeroUsize::MIN,
        TransportLimits::default(),
        BrokerSecurity::Plaintext,
    );
    let expected = identity(1, 10);

    let token = resources
        .open(&poller, expected, address)
        .unwrap_or_else(|error| panic!("open registered resource: {error}"));
    let (_peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept loopback connection: {error}"));
    let mut events = Vec::with_capacity(1);
    let observed = poller
        .poll_into(Some(Duration::from_secs(1)), &mut events)
        .unwrap_or_else(|error| panic!("poll loopback readiness: {error}"));

    assert_eq!(observed, 1);
    assert!(
        matches!(events.as_slice(), [PollEvent::Resource { token: ready, .. }] if *ready == token)
    );
    let Some((found, connection)) = resources.get_mut(token) else {
        panic!("readiness token must resolve the admitted generation");
    };
    assert_eq!(found, expected);
    assert_eq!(
        connection
            .finish_connect()
            .unwrap_or_else(|error| panic!("verify ready connect: {error}")),
        ConnectProgress::Opened
    );
    assert_eq!(resources.len(), 1);
}

#[test]
fn close_deregisters_and_releases_the_exact_identity() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback listener: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback address: {error}"));
    let poller = poller();
    let mut resources = TransportResources::new(
        NonZeroUsize::MIN,
        TransportLimits::default(),
        BrokerSecurity::Plaintext,
    );
    let expected = identity(1, 10);
    let token = resources
        .open(&poller, expected, address)
        .unwrap_or_else(|error| panic!("open registered resource: {error}"));
    let (_peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept loopback connection: {error}"));

    let Ok(removed) = resources.close(&poller, expected) else {
        panic!("registered resource must deregister");
    };
    assert!(removed);
    assert!(resources.get_mut(token).is_none());
    let Ok(removed_again) = resources.close(&poller, expected) else {
        panic!("absent resource close must succeed");
    };
    assert!(!removed_again);
    assert_eq!(resources.len(), 0);
}

fn poller() -> Poller {
    Poller::new(NonZeroUsize::MIN).unwrap_or_else(|error| panic!("create Mio selector: {error}"))
}

fn identity(transport_id: u64, epoch: u64) -> ResourceIdentity {
    ResourceIdentity::new(
        TransportId::from_raw(transport_id),
        ConnectionEpoch::from_raw(epoch),
    )
}
