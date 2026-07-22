//! Sanitized failures while constructing or validating coordinator discovery.

use std::{error::Error, fmt};

use kafka_driver_core::{BrokerIdError, CoordinatorKind, HostNameError};
use kafka_wire_core::ApiVersion;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CoordinatorBuildError {
    UnsupportedKind {
        kind: CoordinatorKind,
        version: ApiVersion,
    },
    ResponseCount {
        observed: usize,
    },
    KeyMismatch,
    Response {
        error_code: i16,
    },
    BrokerId(BrokerIdError),
    BrokerHost(HostNameError),
    BrokerPort {
        port: i32,
    },
}

impl fmt::Display for CoordinatorBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedKind { kind, version } => {
                write!(
                    formatter,
                    "{kind:?} coordinator requires a version newer than {version}"
                )
            }
            Self::ResponseCount { observed } => {
                write!(
                    formatter,
                    "single-key coordinator response contains {observed} results"
                )
            }
            Self::KeyMismatch => {
                formatter.write_str("coordinator response does not match the requested key")
            }
            Self::Response { error_code } => {
                write!(
                    formatter,
                    "coordinator discovery failed with Kafka error {error_code}"
                )
            }
            Self::BrokerId(source) => write!(formatter, "invalid coordinator broker: {source}"),
            Self::BrokerHost(source) => write!(formatter, "invalid coordinator host: {source}"),
            Self::BrokerPort { port } => write!(formatter, "invalid coordinator port {port}"),
        }
    }
}

impl Error for CoordinatorBuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::BrokerId(source) => Some(source),
            Self::BrokerHost(source) => Some(source),
            Self::UnsupportedKind { .. }
            | Self::ResponseCount { .. }
            | Self::KeyMismatch
            | Self::Response { .. }
            | Self::BrokerPort { .. } => None,
        }
    }
}
