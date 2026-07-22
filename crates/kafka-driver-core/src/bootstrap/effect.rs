//! External DNS work and terminal owner outcomes emitted by bootstrap policy.

use crate::{BrokerEndpoint, ConnectionEpoch, DnsFailure, DnsRequest, ResolvedAddressSet};

/// One ordered action or terminal result emitted by a bootstrap transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BootstrapEffect {
    /// Requests external DNS resolution with explicit stale-work identity.
    Resolve {
        /// Exact resolver request to interpret.
        request: DnsRequest,
    },
    /// Returns a bounded address set selected from one configured endpoint.
    Resolved {
        /// Connection generation that owns the result.
        epoch: ConnectionEpoch,
        /// Configured endpoint that produced the addresses.
        endpoint: BrokerEndpoint,
        /// Nonempty addresses retained in resolver preference order.
        addresses: ResolvedAddressSet,
    },
    /// Reports that every configured endpoint failed once in this attempt.
    Exhausted {
        /// Connection generation that exhausted bootstrap membership.
        epoch: ConnectionEpoch,
        /// Sanitized failure returned by the final endpoint.
        last_failure: DnsFailure,
    },
}
