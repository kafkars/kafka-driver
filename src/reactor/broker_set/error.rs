//! Construction and child-delegation failures for the bounded broker set.

use std::{error::Error, fmt};

use crate::reactor::broker::BrokerError;

#[derive(Debug)]
pub(in crate::reactor) enum BrokerSetError {
    OwnerCapacityOverflow,
    DirectoryCapacity { observed: usize, limit: usize },
    NamespaceUnavailable,
    ChildCapacityReached,
    UnknownBrokerChild,
    BrokerTemplateMissing,
    ConnectionEpochExhausted,
    UnexpectedResolutionEffect,
    SeedMissing,
    SeedAlreadyInstalled,
    Broker(BrokerError),
}

impl fmt::Display for BrokerSetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OwnerCapacityOverflow => {
                formatter.write_str("broker owner capacity cannot reserve its seed slot")
            }
            Self::DirectoryCapacity { observed, limit } => write!(
                formatter,
                "broker directory has {observed} entries, set capacity is {limit}"
            ),
            Self::NamespaceUnavailable => {
                formatter.write_str("broker namespace could not be represented")
            }
            Self::ChildCapacityReached => {
                formatter.write_str("discovered broker child capacity reached")
            }
            Self::UnknownBrokerChild => {
                formatter.write_str("resolver outcome named an unknown broker child")
            }
            Self::BrokerTemplateMissing => {
                formatter.write_str("discovered broker connection policy is unavailable")
            }
            Self::ConnectionEpochExhausted => {
                formatter.write_str("broker connection epoch space is exhausted")
            }
            Self::UnexpectedResolutionEffect => {
                formatter.write_str("broker resolution emitted an invalid effect sequence")
            }
            Self::SeedAlreadyInstalled => {
                formatter.write_str("broker set already owns a seed connection")
            }
            Self::SeedMissing => formatter.write_str("broker set has no seed connection"),
            Self::Broker(_) => formatter.write_str("one broker child failed"),
        }
    }
}

impl Error for BrokerSetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Broker(source) => Some(source),
            Self::OwnerCapacityOverflow
            | Self::DirectoryCapacity { .. }
            | Self::NamespaceUnavailable
            | Self::ChildCapacityReached
            | Self::UnknownBrokerChild
            | Self::BrokerTemplateMissing
            | Self::ConnectionEpochExhausted
            | Self::UnexpectedResolutionEffect
            | Self::SeedMissing
            | Self::SeedAlreadyInstalled => None,
        }
    }
}
