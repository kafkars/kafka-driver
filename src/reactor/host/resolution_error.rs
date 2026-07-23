//! Sanitized failures joining DNS ownership, bootstrap policy, and worker admission.

use std::fmt;

use crate::reactor::{
    bootstrap::BootstrapOwnerError,
    resolver::{ResolverOwnershipError, ResolverSubmitError, ResolverWorkerError},
};

#[derive(Debug)]
pub(super) enum NameResolutionError {
    IdentityExhausted,
    ReservationUnavailable,
    PermitMismatch,
    Ownership(ResolverOwnershipError),
    Resolver(ResolverSubmitError),
    Worker(ResolverWorkerError),
    Bootstrap(BootstrapOwnerError),
}

impl fmt::Display for NameResolutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentityExhausted => {
                formatter.write_str("resolver effect identity space is exhausted")
            }
            Self::ReservationUnavailable => {
                formatter.write_str("initial resolver reservation is unavailable")
            }
            Self::PermitMismatch => {
                formatter.write_str("resolver request does not match its reserved effect identity")
            }
            Self::Ownership(error) => error.fmt(formatter),
            Self::Resolver(error) => error.fmt(formatter),
            Self::Worker(error) => error.fmt(formatter),
            Self::Bootstrap(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for NameResolutionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Ownership(error) => Some(error),
            Self::Resolver(error) => Some(error),
            Self::Worker(error) => Some(error),
            Self::Bootstrap(error) => Some(error),
            Self::IdentityExhausted | Self::ReservationUnavailable | Self::PermitMismatch => None,
        }
    }
}

impl From<ResolverOwnershipError> for NameResolutionError {
    fn from(source: ResolverOwnershipError) -> Self {
        Self::Ownership(source)
    }
}

impl From<ResolverSubmitError> for NameResolutionError {
    fn from(source: ResolverSubmitError) -> Self {
        Self::Resolver(source)
    }
}

impl From<ResolverWorkerError> for NameResolutionError {
    fn from(source: ResolverWorkerError) -> Self {
        Self::Worker(source)
    }
}

impl From<BootstrapOwnerError> for NameResolutionError {
    fn from(source: BootstrapOwnerError) -> Self {
        Self::Bootstrap(source)
    }
}
