//! Discovery demand, route invalidation, and identity-fenced external outcomes.

use crate::{BrokerEndpoint, BrokerId, CoordinatorEpoch, CoordinatorRoute, OperationId};

/// One owner command or `FindCoordinator` result applied to coordinator policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoordinatorInput {
    /// Requests a route that an active discovery may satisfy.
    Resolve {
        /// Reserved identity used only if discovery starts.
        operation_id: OperationId,
    },
    /// Requires a discovery newer than the currently authoritative route.
    Refresh {
        /// Reserved identity used only if discovery starts.
        operation_id: OperationId,
    },
    /// Invalidates only the exact route that remains authoritative.
    Invalidate {
        /// Previously issued route token.
        route: CoordinatorRoute,
        /// Reserved identity used only if discovery starts.
        operation_id: OperationId,
    },
    /// Reports one successful coordinator discovery.
    DiscoverySucceeded {
        /// Completed discovery identity.
        operation_id: OperationId,
        /// Discovery epoch returned by external work.
        epoch: CoordinatorEpoch,
        /// Broker identity returned by Kafka.
        broker_id: BrokerId,
        /// Validated endpoint returned by Kafka.
        endpoint: BrokerEndpoint,
        /// Identity reserved if queued refresh demand must start immediately.
        followup_operation_id: OperationId,
    },
    /// Reports a failed or malformed coordinator discovery.
    DiscoveryFailed {
        /// Failed discovery identity.
        operation_id: OperationId,
        /// Discovery epoch returned by external work.
        epoch: CoordinatorEpoch,
        /// Identity reserved if queued refresh demand must retry immediately.
        followup_operation_id: OperationId,
    },
}
