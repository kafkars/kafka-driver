//! External address use or resolution requested by endpoint-dialing policy.

use crate::{BrokerEndpoint, ResolvedAddress};

/// One ordered action emitted by a dialer transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EndpointDialerEffect {
    /// Opens the next retained candidate address.
    OpenCandidate {
        /// Logical endpoint whose address was selected.
        endpoint: BrokerEndpoint,
        /// Candidate retained in resolver preference order.
        address: ResolvedAddress,
    },
    /// Refreshes the logical endpoint after every retained candidate failed.
    Resolve {
        /// Exact logical endpoint to resolve again.
        endpoint: BrokerEndpoint,
    },
}
