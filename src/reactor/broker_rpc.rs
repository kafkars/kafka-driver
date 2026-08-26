//! Selector-neutral request access to one semantically ready Kafka broker.

use std::{fmt, io};

use kafka_driver_core::Moment;
use kafka_wire_core::{ApiKey, ApiVersion};

use crate::request::ErasedRequest;

use super::{
    Poller,
    broker::{BrokerError, SingleBroker},
};

/// Narrow metadata and coordinator RPC access independent of selector ownership.
pub(in crate::reactor) trait BrokerRpc {
    fn is_ready(&self) -> bool;

    fn negotiated_version(&self, api_key: ApiKey) -> Option<ApiVersion>;

    fn submit(
        &mut self,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<(), BrokerRpcError>;
}

/// Sanitized failure from the selector-specific RPC adapter.
#[derive(Debug)]
pub(in crate::reactor) enum BrokerRpcError {
    Legacy(BrokerError),
    Bornera(io::Error),
}

impl fmt::Display for BrokerRpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Legacy(_) => formatter.write_str("legacy broker RPC failed"),
            Self::Bornera(_) => formatter.write_str("Bornera broker RPC failed"),
        }
    }
}

impl std::error::Error for BrokerRpcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Legacy(source) => Some(source),
            Self::Bornera(source) => Some(source),
        }
    }
}

/// Affine adapter joining one legacy broker to its sole owning poller.
pub(in crate::reactor) struct LegacyBrokerRpc<'a> {
    broker: &'a mut SingleBroker,
    poller: &'a Poller,
}

impl<'a> LegacyBrokerRpc<'a> {
    pub(in crate::reactor) const fn new(broker: &'a mut SingleBroker, poller: &'a Poller) -> Self {
        Self { broker, poller }
    }
}

impl BrokerRpc for LegacyBrokerRpc<'_> {
    fn is_ready(&self) -> bool {
        self.broker.state().phase() == kafka_driver_core::ConnectionPhase::Ready
    }

    fn negotiated_version(&self, api_key: ApiKey) -> Option<ApiVersion> {
        self.broker.negotiated_version(api_key)
    }

    fn submit(
        &mut self,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<(), BrokerRpcError> {
        self.broker
            .submit(self.poller, request, now)
            .map_err(BrokerRpcError::Legacy)
    }
}
