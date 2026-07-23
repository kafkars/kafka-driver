//! Discovery demand, route invalidation, and identity-fenced external outcomes.

use crate::{
    BrokerEndpoint, BrokerId, CoordinatorEpoch, CoordinatorRoute, EvidenceStamp, OperationId,
    OutcomeStamp,
};

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
        /// Position at which the failed broker response became observable.
        observed_at: OutcomeStamp,
        /// Reserved identity used only if discovery starts.
        operation_id: OperationId,
    },
    /// Withdraws an exact route after its advertised broker endpoint disappears.
    Withdraw {
        /// Exact currently installed route.
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
        /// Position at which this external discovery began.
        evidence: EvidenceStamp,
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
