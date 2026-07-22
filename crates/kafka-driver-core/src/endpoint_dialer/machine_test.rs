//! Scenarios for candidate order, readiness preference, and DNS refresh demand.

use std::num::NonZeroU16;

use crate::{
    BrokerEndpoint, HostName, IpAddress, ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
};

use super::{EndpointDialer, EndpointDialerEffect, EndpointDialerInput};

#[test]
fn every_failed_candidate_is_selected_once_before_reresolution() {
    let mut dialer = dialer();

    let first = dialer.apply(EndpointDialerInput::OpenCandidate);
    let first_failure = dialer.apply(EndpointDialerInput::ConnectionFailed);
    let second = dialer.apply(EndpointDialerInput::OpenCandidate);
    let second_failure = dialer.apply(EndpointDialerInput::ConnectionFailed);

    assert_eq!(opened(&first), Some([127, 0, 0, 2]));
    assert!(first_failure.effects().is_empty());
    assert_eq!(opened(&second), Some([127, 0, 0, 1]));
    assert_eq!(
        second_failure.effects(),
        [EndpointDialerEffect::Resolve {
            endpoint: endpoint(),
        }]
    );
}

#[test]
fn a_previously_ready_address_is_retried_before_rotating() {
    let mut dialer = dialer();
    let _ = dialer.apply(EndpointDialerInput::OpenCandidate);
    let _ = dialer.apply(EndpointDialerInput::ConnectionReady);
    let _ = dialer.apply(EndpointDialerInput::ConnectionFailed);

    let retry = dialer.apply(EndpointDialerInput::OpenCandidate);

    assert_eq!(opened(&retry), Some([127, 0, 0, 2]));
}

#[test]
fn refreshed_addresses_restart_selection_from_new_resolver_order() {
    let mut dialer = dialer();
    let _ = dialer.apply(EndpointDialerInput::OpenCandidate);
    let refreshed = addresses([[10, 0, 0, 1], [10, 0, 0, 2]]);
    let _ = dialer.apply(EndpointDialerInput::ResolutionCompleted {
        addresses: refreshed,
    });

    let selected = dialer.apply(EndpointDialerInput::OpenCandidate);

    assert_eq!(opened(&selected), Some([10, 0, 0, 1]));
}

fn dialer() -> EndpointDialer {
    EndpointDialer::new(endpoint(), addresses([[127, 0, 0, 2], [127, 0, 0, 1]]))
}

fn endpoint() -> BrokerEndpoint {
    let host = HostName::new("broker.test").unwrap_or_else(|error| panic!("valid host: {error}"));
    BrokerEndpoint::new(host, port())
}

fn addresses<const N: usize>(octets: [[u8; 4]; N]) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        octets.map(|octets| ResolvedAddress::new(IpAddress::V4(octets), port())),
        ResolutionLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid addresses: {error}"))
}

fn opened(transition: &super::EndpointDialerTransition) -> Option<[u8; 4]> {
    match transition.effects() {
        [EndpointDialerEffect::OpenCandidate { address, .. }] => match address.ip() {
            IpAddress::V4(octets) => Some(octets),
            IpAddress::V6(_) => None,
        },
        _ => None,
    }
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}
