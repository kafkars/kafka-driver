//! Selector-neutral RPC access to one Bornera-owned Kafka lane.

use bornera::RegisteredTransport;
use kafka_driver_core::Moment;
use kafka_wire_core::{ApiKey, ApiVersion};

use crate::{
    reactor::{
        broker_rpc::{BrokerRpc, BrokerRpcError},
        causality::CausalSequence,
    },
    request::ErasedRequest,
};

use super::owner::DirectLaneAccess;

/// Affine adapter joining a direct lane to the host causal sequence.
pub(in crate::reactor) struct DirectBrokerRpc<'lane, 'cause, T: RegisteredTransport> {
    lane: DirectLaneAccess<'lane, T>,
    causality: &'cause mut CausalSequence,
}

impl<'lane, 'cause, T: RegisteredTransport> DirectBrokerRpc<'lane, 'cause, T> {
    pub(super) const fn new(
        lane: DirectLaneAccess<'lane, T>,
        causality: &'cause mut CausalSequence,
    ) -> Self {
        Self { lane, causality }
    }
}

#[cfg(test)]
#[path = "rpc_test.rs"]
mod test;

impl<T: RegisteredTransport> BrokerRpc for DirectBrokerRpc<'_, '_, T> {
    fn is_ready(&self) -> bool {
        self.lane.can_admit_public()
    }

    fn negotiated_version(&self, api_key: ApiKey) -> Option<ApiVersion> {
        self.lane.session.negotiated_version(api_key)
    }

    fn submit(
        &mut self,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<(), BrokerRpcError> {
        self.lane
            .submit_request(request, now, self.causality)
            .map_err(BrokerRpcError::Bornera)
    }
}
