//! Address-pass mutation boundaries for terminal and retryable close reasons.

use std::{net::SocketAddr, num::NonZeroU16};

use kafka_driver_core::{
    AuthenticationFailure, BrokerEndpoint, CloseReason, HostName, IpAddress, ResolutionLimits,
    ResolvedAddress, ResolvedAddressSet,
};

use crate::{config::BrokerAddresses, reactor::address_rotation::AddressRotation};

use super::endpoint_refresh::failed_endpoint;

#[test]
fn permanent_authentication_and_requested_shutdown_do_not_consume_a_candidate() {
    for reason in [
        CloseReason::Requested,
        CloseReason::Drained,
        CloseReason::AuthenticationFailed(AuthenticationFailure::Rejected),
    ] {
        let mut addresses = rotation();
        assert_eq!(addresses.next(), Some(socket()));
        assert_eq!(failed_endpoint(&mut addresses, reason), None);
        assert_eq!(addresses.failed(), Some(endpoint()));
    }
}

#[test]
fn retryable_authentication_failure_consumes_the_unready_candidate() {
    let mut addresses = rotation();
    assert_eq!(addresses.next(), Some(socket()));
    assert_eq!(
        failed_endpoint(
            &mut addresses,
            CloseReason::AuthenticationFailed(AuthenticationFailure::Timeout),
        ),
        Some(endpoint())
    );
}

fn rotation() -> AddressRotation {
    let addresses = ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(IpAddress::V4([127, 0, 0, 1]), port())],
        ResolutionLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid resolved address: {error}"));
    AddressRotation::new(BrokerAddresses::Resolved {
        endpoint: endpoint(),
        addresses,
    })
}

fn endpoint() -> BrokerEndpoint {
    BrokerEndpoint::new(
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid host: {error}")),
        port(),
    )
}

fn socket() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port().get()))
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port is nonzero");
    };
    port
}
