//! Internal bootstrap effect-interpretation failures without endpoint leakage.

use std::fmt;

use super::super::resolver::ResolverSubmitError;

/// Why reactor bootstrap ownership could not preserve its machine contract.
#[derive(Debug)]
pub(in crate::reactor) enum BootstrapOwnerError {
    IdentityExhausted,
    Resolver(ResolverSubmitError),
    UnexpectedEffect,
}

impl fmt::Display for BootstrapOwnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentityExhausted => {
                formatter.write_str("bootstrap effect identity space is exhausted")
            }
            Self::Resolver(error) => error.fmt(formatter),
            Self::UnexpectedEffect => {
                formatter.write_str("bootstrap machine emitted an invalid effect sequence")
            }
        }
    }
}

impl std::error::Error for BootstrapOwnerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Resolver(source) => Some(source),
            Self::IdentityExhausted | Self::UnexpectedEffect => None,
        }
    }
}

impl From<ResolverSubmitError> for BootstrapOwnerError {
    fn from(source: ResolverSubmitError) -> Self {
        Self::Resolver(source)
    }
}
