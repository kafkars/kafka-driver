//! Explicit bounded-admission failures that return resource ownership.

use std::fmt;

use kafka_driver_core::TransportId;

/// Why an I/O resource could not enter the reactor registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum ResourceAdmissionFailure {
    /// Another live resource already owns the transport identity.
    IdentityInUse { transport_id: TransportId },
    /// Every configured slot currently owns a live resource.
    CapacityReached { limit: usize },
    /// Every vacant slot exhausted its stale-event-safe token generations.
    TokenSpaceExhausted,
}

impl fmt::Display for ResourceAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentityInUse { transport_id } => write!(
                formatter,
                "transport identity {} is already registered",
                transport_id.get()
            ),
            Self::CapacityReached { limit } => {
                write!(formatter, "I/O resource capacity {limit} has been reached")
            }
            Self::TokenSpaceExhausted => {
                formatter.write_str("I/O resource token generations have been exhausted")
            }
        }
    }
}

impl std::error::Error for ResourceAdmissionFailure {}

/// Failed resource admission with ownership of the unregistered value.
#[derive(Debug)]
pub(in crate::reactor) struct ResourceAdmissionError<R> {
    failure: ResourceAdmissionFailure,
    resource: R,
}

impl<R> ResourceAdmissionError<R> {
    pub(super) const fn new(failure: ResourceAdmissionFailure, resource: R) -> Self {
        Self { failure, resource }
    }

    pub(in crate::reactor) const fn failure(&self) -> ResourceAdmissionFailure {
        self.failure
    }

    pub(in crate::reactor) fn into_resource(self) -> R {
        self.resource
    }
}

impl<R> fmt::Display for ResourceAdmissionError<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.failure.fmt(formatter)
    }
}

impl<R: fmt::Debug> std::error::Error for ResourceAdmissionError<R> {}
