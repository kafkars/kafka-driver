//! Scenarios for retaining logical resolver address fields at the socket boundary.

use std::{net::SocketAddr, num::NonZeroU16};

use kafka_driver_core::{IpAddress, ResolvedAddress};

use super::socket_address;

#[test]
fn ipv4_address_becomes_the_same_socket_endpoint() {
    let resolved = ResolvedAddress::new(IpAddress::V4([127, 0, 0, 1]), port(9092));

    let socket = socket_address(resolved);

    assert_eq!(socket, SocketAddr::from(([127, 0, 0, 1], 9092)));
}

#[test]
fn ipv6_flow_and_scope_survive_socket_conversion() {
    let resolved = ResolvedAddress::ipv6(
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
        port(9093),
        7,
        11,
    );

    let SocketAddr::V6(socket) = socket_address(resolved) else {
        panic!("IPv6 resolver result must remain IPv6");
    };

    assert_eq!(socket.port(), 9093);
    assert_eq!(socket.flowinfo(), 7);
    assert_eq!(socket.scope_id(), 11);
}

fn port(raw: u16) -> NonZeroU16 {
    NonZeroU16::new(raw).unwrap_or_else(|| panic!("test port must be nonzero"))
}
