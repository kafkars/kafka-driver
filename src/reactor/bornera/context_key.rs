//! Exact Bornera lifetime identity for one published semantic context.

use bornera_core::{ConnectionEpoch, OperationId};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::reactor) struct OperationContextKey {
    epoch: ConnectionEpoch,
    operation: OperationId,
}

impl OperationContextKey {
    pub(in crate::reactor) const fn new(epoch: ConnectionEpoch, operation: OperationId) -> Self {
        Self { epoch, operation }
    }

    pub(in crate::reactor) const fn epoch(self) -> ConnectionEpoch {
        self.epoch
    }

    pub(in crate::reactor) const fn operation(self) -> OperationId {
        self.operation
    }
}
