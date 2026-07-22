//! Public failure from driving the operating-system poll selector.

use std::{fmt, io};

use super::{broker_set::BrokerSetError, clock::ClockOverflow, metadata::MetadataOwnerError};

/// Why one reactor turn could not observe external readiness.
#[derive(Debug)]
pub struct ReactorError {
    source: io::Error,
    operation: ReactorOperation,
}

impl ReactorError {
    pub(super) const fn poll(source: io::Error) -> Self {
        Self {
            source,
            operation: ReactorOperation::Poll,
        }
    }

    pub(super) fn broker_set(source: BrokerSetError) -> Self {
        Self {
            source: io::Error::other(source),
            operation: ReactorOperation::BrokerSet,
        }
    }

    pub(super) fn clock(source: ClockOverflow) -> Self {
        Self {
            source: io::Error::other(source),
            operation: ReactorOperation::Clock,
        }
    }

    pub(super) fn metadata(source: MetadataOwnerError) -> Self {
        Self {
            source: io::Error::other(source),
            operation: ReactorOperation::Metadata,
        }
    }

    pub(super) const fn host(source: io::Error) -> Self {
        Self {
            source,
            operation: ReactorOperation::Host,
        }
    }
}

impl fmt::Display for ReactorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.operation {
            ReactorOperation::Poll => formatter.write_str("the driver I/O selector failed"),
            ReactorOperation::BrokerSet => formatter.write_str("the broker set failed"),
            ReactorOperation::Clock => formatter.write_str("the driver clock failed"),
            ReactorOperation::Metadata => formatter.write_str("the cluster metadata owner failed"),
            ReactorOperation::Host => formatter.write_str("the reactor host invariant failed"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ReactorOperation {
    Poll,
    BrokerSet,
    Clock,
    Metadata,
    Host,
}

impl std::error::Error for ReactorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
