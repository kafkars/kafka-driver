//! Refresh demand, route invalidation, and identity-fenced Metadata RPC outcomes.

use crate::{BrokerRoute, MetadataQuery, MetadataSnapshot, OperationId};

/// One owner command or generated Metadata RPC result applied to metadata policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataInput {
    /// Requests facts that an identical in-flight or queued query may satisfy.
    Resolve {
        /// Exact cluster or topic facts required by the caller.
        query: MetadataQuery,
        /// Reserved logical operation identity used only if a fetch starts.
        operation_id: OperationId,
    },
    /// Requires a query newer than an identical fetch already in flight.
    Refresh {
        /// Exact cluster or topic facts that must be refreshed.
        query: MetadataQuery,
        /// Reserved logical operation identity used only if a fetch starts.
        operation_id: OperationId,
    },
    /// Invalidates a route only when its issuing generation remains current.
    InvalidateBrokerRoute {
        /// Route token whose generation authorizes invalidation.
        route: BrokerRoute,
        /// Reserved operation identity used only if a fetch starts.
        operation_id: OperationId,
    },
    /// Installs one coherent snapshot after exact operation and generation checks.
    RefreshSucceeded {
        /// Completed refresh operation identity.
        operation_id: OperationId,
        /// Coherent immutable snapshot produced by the response.
        snapshot: MetadataSnapshot,
        /// Identity reserved if coalesced demand requires an immediate follow-up.
        followup_operation_id: OperationId,
    },
    /// Reports that one refresh ended without an installable snapshot.
    RefreshFailed {
        /// Failed refresh operation identity.
        operation_id: OperationId,
        /// Identity reserved if another queued query can start immediately.
        followup_operation_id: OperationId,
    },
}
