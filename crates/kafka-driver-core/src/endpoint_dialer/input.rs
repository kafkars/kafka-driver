//! Connection outcomes and refreshed addresses accepted by endpoint policy.

use crate::ResolvedAddressSet;

/// One deterministic endpoint-dialing input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EndpointDialerInput {
    /// Selects one candidate for a fresh connection epoch.
    OpenCandidate,
    /// Reports that the selected candidate reached Kafka readiness.
    ConnectionReady,
    /// Reports that the selected candidate failed before readiness or was later lost.
    ConnectionFailed,
    /// Replaces candidates with one fresh bounded resolver result.
    ResolutionCompleted {
        /// New nonempty candidates in resolver preference order.
        addresses: ResolvedAddressSet,
    },
}
