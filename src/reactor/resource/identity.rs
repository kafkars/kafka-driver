//! Typed Kafka connection identity stored alongside generic resource tokens.

use kafka_driver_core::{ConnectionEpoch, TransportId};

/// Identities a readiness event must echo before it can affect a connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct ResourceIdentity {
    transport_id: TransportId,
    epoch: ConnectionEpoch,
}

impl ResourceIdentity {
    pub(in crate::reactor) const fn new(transport_id: TransportId, epoch: ConnectionEpoch) -> Self {
        Self {
            transport_id,
            epoch,
        }
    }

    pub(in crate::reactor) const fn transport_id(self) -> TransportId {
        self.transport_id
    }

    pub(in crate::reactor) const fn epoch(self) -> ConnectionEpoch {
        self.epoch
    }
}
