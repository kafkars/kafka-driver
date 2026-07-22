//! Sanitization of external socket and registry failures before machine input.

use std::io;

use kafka_driver_core::TransportFailure;

#[cfg(feature = "tls-rustls")]
use crate::reactor::tls::{TlsConnectError, TlsError};
use crate::reactor::{
    plaintext::PlaintextError,
    resource::{ResourceAdmissionFailure, TransportOpenError},
    transport::{TransportConnectError, TransportError},
};

pub(super) fn open_failure(error: &TransportOpenError) -> TransportFailure {
    match error {
        TransportOpenError::Connect(source) => connect_failure(source),
        TransportOpenError::Register(source) => io_failure(source),
        TransportOpenError::Admission(
            ResourceAdmissionFailure::IdentityInUse { .. }
            | ResourceAdmissionFailure::CapacityReached { .. }
            | ResourceAdmissionFailure::TokenSpaceExhausted,
        )
        | TransportOpenError::RegistryInvariant => TransportFailure::Other,
    }
}

fn connect_failure(error: &TransportConnectError) -> TransportFailure {
    match error {
        TransportConnectError::Plaintext(source) => io_failure(source),
        #[cfg(feature = "tls-rustls")]
        TransportConnectError::Rustls(source) => tls_connect_failure(source),
    }
}

pub(super) fn transport_failure(error: &TransportError) -> TransportFailure {
    match error {
        TransportError::Plaintext(source) => plaintext_failure(source),
        #[cfg(feature = "tls-rustls")]
        TransportError::Rustls(source) => tls_failure(source),
    }
}

fn plaintext_failure(error: &PlaintextError) -> TransportFailure {
    match error {
        PlaintextError::Connect(source)
        | PlaintextError::Read(source)
        | PlaintextError::Write(source) => io_failure(source),
        PlaintextError::WriteZero | PlaintextError::Frame(_) | PlaintextError::WriteProgress(_) => {
            TransportFailure::Other
        }
    }
}

#[cfg(feature = "tls-rustls")]
fn tls_connect_failure(error: &TlsConnectError) -> TransportFailure {
    match error {
        TlsConnectError::Tcp(source) => io_failure(source),
        TlsConnectError::Session(_) => TransportFailure::Other,
    }
}

#[cfg(feature = "tls-rustls")]
fn tls_failure(error: &TlsError) -> TransportFailure {
    match error {
        TlsError::TcpConnect(source) | TlsError::TlsRead(source) | TlsError::TlsWrite(source) => {
            io_failure(source)
        }
        TlsError::PlaintextRead(_)
        | TlsError::PlaintextWrite(_)
        | TlsError::Protocol(_)
        | TlsError::WriteZero
        | TlsError::Frame(_)
        | TlsError::WriteProgress(_) => TransportFailure::Other,
    }
}

fn io_failure(error: &io::Error) -> TransportFailure {
    match error.kind() {
        io::ErrorKind::ConnectionRefused => TransportFailure::Refused,
        io::ErrorKind::ConnectionReset
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::NotConnected
        | io::ErrorKind::UnexpectedEof => TransportFailure::Reset,
        _ => TransportFailure::Other,
    }
}
