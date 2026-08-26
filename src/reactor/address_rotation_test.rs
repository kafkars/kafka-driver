//! Scenarios for stable bounded address order across connection attempts.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use kafka_driver_core::{
    BrokerEndpoint, HostName, IpAddress, ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
};

use crate::config::BrokerAddresses;

use super::address_rotation::AddressRotation;

#[test]
fn given_resolver_order_when_epochs_open_then_each_address_is_tried_before_wrapping() {
    // Given
    let first = resolved([127, 0, 0, 2]);
    let second = resolved([127, 0, 0, 1]);
    let addresses = ResolvedAddressSet::try_from_iter([first, second], ResolutionLimits::default())
        .unwrap_or_else(|error| panic!("valid test addresses: {error}"));
    let mut rotation = AddressRotation::new(BrokerAddresses::Resolved {
        endpoint: endpoint(),
        addresses,
    });

    // When / Then
    assert_eq!(rotation.primary(), Some(socket([127, 0, 0, 2])));
    assert_eq!(rotation.next(), Some(socket([127, 0, 0, 2])));
    assert!(rotation.failed().is_none());
    assert_eq!(rotation.next(), Some(socket([127, 0, 0, 1])));
    assert_eq!(rotation.failed(), Some(endpoint()));
    assert_eq!(rotation.next(), Some(socket([127, 0, 0, 2])));
}

#[test]
fn given_a_ready_candidate_when_it_later_fails_then_it_is_preferred_again() {
    let addresses = ResolvedAddressSet::try_from_iter(
        [resolved([127, 0, 0, 2]), resolved([127, 0, 0, 1])],
        ResolutionLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid test addresses: {error}"));
    let mut rotation = AddressRotation::new(BrokerAddresses::Resolved {
        endpoint: endpoint(),
        addresses,
    });

    assert_eq!(rotation.next(), Some(socket([127, 0, 0, 2])));
    rotation.ready();
    assert!(rotation.failed().is_none());
    assert_eq!(rotation.next(), Some(socket([127, 0, 0, 2])));
}

fn resolved(octets: [u8; 4]) -> ResolvedAddress {
    ResolvedAddress::new(
        IpAddress::V4(octets),
        std::num::NonZeroU16::new(9092).unwrap_or_else(|| panic!("port is nonzero")),
    )
}

fn socket(octets: [u8; 4]) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::from(octets)), 9092)
}

fn endpoint() -> BrokerEndpoint {
    let host = HostName::new("broker.test").unwrap_or_else(|error| panic!("valid host: {error}"));
    BrokerEndpoint::new(
        host,
        std::num::NonZeroU16::new(9092).unwrap_or_else(|| panic!("port is nonzero")),
    )
}
