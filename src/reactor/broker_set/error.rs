//! Construction and child-delegation failures for the bounded broker set.

use std::{error::Error, fmt};

use crate::reactor::broker::BrokerError;

#[derive(Debug)]
pub(in crate::reactor) enum BrokerSetError {
    OwnerCapacityOverflow,
    NamespaceUnavailable,
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
            Self::NamespaceUnavailable => {
                formatter.write_str("seed broker namespace could not be represented")
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
            | Self::NamespaceUnavailable
            | Self::SeedMissing
            | Self::SeedAlreadyInstalled => None,
        }
    }
}
