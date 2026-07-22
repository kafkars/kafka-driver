//! External DNS work and terminal route-scoped resolution results.

use crate::{
    BrokerEndpoint, BrokerRoute, ConnectionEpoch, DnsFailure, DnsRequest, ResolvedAddressSet,
};

/// One ordered external action or terminal broker-resolution result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrokerResolutionEffect {
    /// Requests nonblocking-owner DNS interpretation.
    Resolve {
        /// Exact request whose outcome must echo its identities.
        request: DnsRequest,
    },
    /// Returns usable addresses for the exact advertised route generation.
    Resolved {
        /// Route whose generation authorized the endpoint.
        route: BrokerRoute,
        /// Connection generation that will own the child.
        epoch: ConnectionEpoch,
        /// Advertised endpoint that produced the addresses.
        endpoint: BrokerEndpoint,
        /// Bounded nonempty resolver result.
        addresses: ResolvedAddressSet,
    },
    /// Reports sanitized resolution failure for the exact route.
    Failed {
        /// Route whose activation failed.
        route: BrokerRoute,
        /// Sanitized resolver failure.
        failure: DnsFailure,
    },
}
