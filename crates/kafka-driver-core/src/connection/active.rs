//! Mutable state owned exclusively by one negotiated connection epoch.

use crate::{ConnectionEpoch, NegotiatedCapabilities, TransportId};

use super::{ConnectionLimits, CorrelationAllocator, PendingCall, PendingQueue};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ActiveMode {
    Ready,
    Draining,
}

pub(super) struct ActiveConnection {
    pub(super) epoch: ConnectionEpoch,
    pub(super) transport_id: TransportId,
    pub(super) correlations: CorrelationAllocator,
    pub(super) pending: PendingQueue,
    pub(super) capabilities: NegotiatedCapabilities,
}

impl ActiveConnection {
    pub(super) fn new(
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        capabilities: NegotiatedCapabilities,
        limits: ConnectionLimits,
    ) -> Self {
        Self {
            epoch,
            transport_id,
            correlations: CorrelationAllocator::default(),
            pending: PendingQueue::new(limits.max_in_flight().get()),
            capabilities,
        }
    }

    pub(super) fn pending_calls(&self) -> impl ExactSizeIterator<Item = &PendingCall> {
        self.pending.iter()
    }
}
