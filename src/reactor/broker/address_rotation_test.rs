//! Scenarios for stable bounded address order across connection attempts.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use kafka_driver_core::{IpAddress, ResolutionLimits, ResolvedAddress, ResolvedAddressSet};

use crate::config::BrokerAddresses;

use super::address_rotation::AddressRotation;

#[test]
fn given_resolver_order_when_epochs_open_then_each_address_is_tried_before_wrapping() {
    // Given
    let first = resolved([127, 0, 0, 2]);
    let second = resolved([127, 0, 0, 1]);
    let addresses = ResolvedAddressSet::try_from_iter([first, second], ResolutionLimits::default())
        .unwrap_or_else(|error| panic!("valid test addresses: {error}"));
    let mut rotation = AddressRotation::new(BrokerAddresses::Resolved(addresses));

    // When / Then
    assert_eq!(rotation.primary(), socket([127, 0, 0, 2]));
    assert_eq!(rotation.next(), socket([127, 0, 0, 2]));
    assert_eq!(rotation.next(), socket([127, 0, 0, 1]));
    assert_eq!(rotation.next(), socket([127, 0, 0, 2]));
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
