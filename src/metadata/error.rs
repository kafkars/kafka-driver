//! Sanitized failures while validating generated cluster membership.

use std::{error::Error, fmt};

use kafka_driver_core::{
    BrokerDirectoryError, BrokerId, BrokerIdError, HostNameError, MetadataSnapshotError,
};

/// Why a generated Metadata response could not become immutable driver facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MetadataBuildError {
    Response {
        error_code: i16,
    },
    BrokerCapacity {
        observed: usize,
        limit: usize,
    },
    BrokerId(BrokerIdError),
    BrokerHost {
        broker_id: BrokerId,
        source: HostNameError,
    },
    BrokerPort {
        broker_id: BrokerId,
        port: i32,
    },
    Directory(BrokerDirectoryError),
    ControllerId(BrokerIdError),
    Snapshot(MetadataSnapshotError),
}

impl fmt::Display for MetadataBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Response { error_code } => {
                write!(
                    formatter,
                    "Metadata response failed with Kafka error {error_code}"
                )
            }
            Self::BrokerCapacity { observed, limit } => write!(
                formatter,
                "Metadata response advertises {observed} brokers, limit is {limit}"
            ),
            Self::BrokerId(source) => write!(formatter, "invalid metadata broker: {source}"),
            Self::BrokerHost { broker_id, source } => write!(
                formatter,
                "invalid host advertised for broker {}: {source}",
                broker_id.get()
            ),
            Self::BrokerPort { broker_id, port } => write!(
                formatter,
                "invalid port {port} advertised for broker {}",
                broker_id.get()
            ),
            Self::Directory(source) => write!(formatter, "invalid broker membership: {source}"),
            Self::ControllerId(source) => {
                write!(formatter, "invalid metadata controller: {source}")
            }
            Self::Snapshot(source) => write!(formatter, "incoherent cluster metadata: {source}"),
        }
    }
}

impl Error for MetadataBuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::BrokerId(source) | Self::ControllerId(source) => Some(source),
            Self::BrokerHost { source, .. } => Some(source),
            Self::Directory(source) => Some(source),
            Self::Snapshot(source) => Some(source),
            Self::Response { .. } | Self::BrokerCapacity { .. } | Self::BrokerPort { .. } => None,
        }
    }
}
