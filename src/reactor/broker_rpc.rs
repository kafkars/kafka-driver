//! Selector-neutral request access to one semantically ready Kafka broker.

use std::{fmt, io};

use kafka_driver_core::Moment;
use kafka_wire_core::{ApiKey, ApiVersion};

use crate::request::ErasedRequest;

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
    Bornera(io::Error),
}

impl fmt::Display for BrokerRpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bornera(_) => formatter.write_str("Bornera broker RPC failed"),
        }
    }
}

impl std::error::Error for BrokerRpcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bornera(source) => Some(source),
        }
    }
}
