//! Sanitized failures joining DNS ownership, bootstrap policy, and worker admission.

use std::fmt;

use crate::reactor::{
    bootstrap::BootstrapOwnerError,
    resolver::{ResolverOwnershipError, ResolverSubmitError},
};

#[derive(Debug)]
pub(super) enum NameResolutionError {
    IdentityExhausted,
    Ownership(ResolverOwnershipError),
    Resolver(ResolverSubmitError),
    Bootstrap(BootstrapOwnerError),
}

impl fmt::Display for NameResolutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentityExhausted => {
                formatter.write_str("resolver effect identity space is exhausted")
            }
            Self::Ownership(error) => error.fmt(formatter),
            Self::Resolver(error) => error.fmt(formatter),
            Self::Bootstrap(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for NameResolutionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Ownership(error) => Some(error),
            Self::Resolver(error) => Some(error),
            Self::Bootstrap(error) => Some(error),
            Self::IdentityExhausted => None,
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

impl From<BootstrapOwnerError> for NameResolutionError {
    fn from(source: BootstrapOwnerError) -> Self {
        Self::Bootstrap(source)
    }
}
