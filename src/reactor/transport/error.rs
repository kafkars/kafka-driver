//! Selected broker-stream construction and byte-progress failures.

use std::{fmt, io};

use crate::reactor::plaintext::PlaintextError;
#[cfg(feature = "tls-rustls")]
use crate::reactor::tls::{TlsConnectError, TlsError};

#[derive(Debug)]
pub(in crate::reactor) enum TransportConnectError {
    Plaintext(io::Error),
    #[cfg(feature = "tls-rustls")]
    Rustls(TlsConnectError),
}

impl fmt::Display for TransportConnectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plaintext(_) => formatter.write_str("plaintext connect creation failed"),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for TransportConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Plaintext(source) => Some(source),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(source) => Some(source),
        }
    }
}

#[derive(Debug)]
pub(in crate::reactor) enum TransportError {
    Plaintext(PlaintextError),
    #[cfg(feature = "tls-rustls")]
    Rustls(TlsError),
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plaintext(error) => error.fmt(formatter),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Plaintext(source) => Some(source),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(source) => Some(source),
        }
    }
}
