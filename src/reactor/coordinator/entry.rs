//! One coordinator machine paired with at most one generated response owner.

use kafka_driver_core::{CoordinatorEpoch, CoordinatorMachine, OperationId};
use kafka_wire::FindCoordinatorResponse;
use kafka_wire_core::ApiVersion;

use crate::{Call, RequestError};

use super::invalidation_wait::CoordinatorInvalidation;

pub(super) struct CoordinatorEntry {
    pub(super) machine: CoordinatorMachine,
    pub(super) pending: Option<PendingCoordinator>,
    pub(super) discovery_requested: bool,
    pub(super) invalidation: Option<CoordinatorInvalidation>,
}

impl CoordinatorEntry {
    pub(super) fn new(machine: CoordinatorMachine) -> Self {
        Self {
            machine,
            pending: None,
            discovery_requested: false,
            invalidation: None,
        }
    }
}

pub(super) struct PendingCoordinator {
    pub(super) operation_id: OperationId,
    pub(super) epoch: CoordinatorEpoch,
    pub(super) version: ApiVersion,
    pub(super) call: Call<Result<FindCoordinatorResponse, RequestError>>,
}
