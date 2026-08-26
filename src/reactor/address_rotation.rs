//! Deterministic socket selection for one direct or resolved broker endpoint.

use std::net::SocketAddr;

use kafka_driver_core::{
    BrokerEndpoint, EndpointDialer, EndpointDialerEffect, EndpointDialerInput, ResolvedAddressSet,
};

use crate::config::BrokerAddresses;

use super::resolver::socket_address;

/// Reactor-local adapter over one direct address or deterministic DNS policy.
#[derive(Debug)]
pub(super) enum AddressRotation {
    Direct(SocketAddr),
    Resolved(EndpointDialer),
}

impl AddressRotation {
    pub(super) fn new(addresses: BrokerAddresses) -> Self {
        match addresses {
            BrokerAddresses::Direct(address) => Self::Direct(address),
            BrokerAddresses::Resolved {
                endpoint,
                addresses,
            } => Self::Resolved(EndpointDialer::new(endpoint, addresses)),
        }
    }

    pub(super) fn primary(&self) -> Option<SocketAddr> {
        match self {
            Self::Direct(address) => Some(*address),
            Self::Resolved(dialer) => dialer.primary().map(socket_address),
        }
    }

    pub(super) fn next(&mut self) -> Option<SocketAddr> {
        match self {
            Self::Direct(address) => Some(*address),
            Self::Resolved(dialer) => match dialer
                .apply(EndpointDialerInput::OpenCandidate)
                .into_effects()
                .as_slice()
            {
                [EndpointDialerEffect::OpenCandidate { address, .. }] => {
                    Some(socket_address(*address))
                }
                _ => None,
            },
        }
    }

    pub(super) fn ready(&mut self) {
        if let Self::Resolved(dialer) = self {
            let _ = dialer.apply(EndpointDialerInput::ConnectionReady);
        }
    }

    pub(super) fn failed(&mut self) -> Option<BrokerEndpoint> {
        let Self::Resolved(dialer) = self else {
            return None;
        };
        match dialer
            .apply(EndpointDialerInput::ConnectionFailed)
            .into_effects()
            .as_slice()
        {
            [EndpointDialerEffect::Resolve { endpoint }] => Some(endpoint.clone()),
            _ => None,
        }
    }

    pub(super) fn replace(&mut self, endpoint: BrokerEndpoint, addresses: ResolvedAddressSet) {
        *self = Self::Resolved(EndpointDialer::new(endpoint, addresses));
    }
}
