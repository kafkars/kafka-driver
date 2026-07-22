//! Named coordinator transition results shared by admission and outcomes.

use crate::{CoordinatorEpoch, CoordinatorKey, OperationId};

use super::{CoordinatorDisposition, CoordinatorEffect, CoordinatorTransition};

pub(super) fn find(
    operation_id: OperationId,
    key: CoordinatorKey,
    epoch: CoordinatorEpoch,
) -> CoordinatorTransition {
    CoordinatorTransition::new(
        vec![CoordinatorEffect::Find {
            operation_id,
            key,
            epoch,
        }],
        CoordinatorDisposition::Applied,
    )
}

pub(super) fn applied() -> CoordinatorTransition {
    CoordinatorTransition::new(Vec::new(), CoordinatorDisposition::Applied)
}

pub(super) fn known() -> CoordinatorTransition {
    CoordinatorTransition::new(Vec::new(), CoordinatorDisposition::AlreadyKnown)
}

pub(super) fn coalesced() -> CoordinatorTransition {
    CoordinatorTransition::new(Vec::new(), CoordinatorDisposition::Coalesced)
}

pub(super) fn queued() -> CoordinatorTransition {
    CoordinatorTransition::new(Vec::new(), CoordinatorDisposition::RefreshQueued)
}

pub(super) fn stale() -> CoordinatorTransition {
    CoordinatorTransition::new(Vec::new(), CoordinatorDisposition::IgnoredStale)
}

pub(super) fn exhausted() -> CoordinatorTransition {
    CoordinatorTransition::new(
        vec![CoordinatorEffect::EpochExhausted],
        CoordinatorDisposition::Applied,
    )
}
