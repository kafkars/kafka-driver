//! Sanitization of external socket and registry failures before machine input.

use std::io;

use kafka_driver_core::TransportFailure;

use crate::reactor::{
    plaintext::PlaintextError,
    resource::{ResourceAdmissionFailure, ResourceOpenError},
};

pub(super) fn open_failure(error: &ResourceOpenError) -> TransportFailure {
    match error {
        ResourceOpenError::Connect(source) | ResourceOpenError::Register(source) => {
            io_failure(source)
        }
        ResourceOpenError::Admission(
            ResourceAdmissionFailure::IdentityInUse { .. }
            | ResourceAdmissionFailure::CapacityReached { .. }
            | ResourceAdmissionFailure::TokenSpaceExhausted,
        )
        | ResourceOpenError::RegistryInvariant => TransportFailure::Other,
    }
}

pub(super) fn plaintext_failure(error: &PlaintextError) -> TransportFailure {
    match error {
        PlaintextError::Connect(source)
        | PlaintextError::Read(source)
        | PlaintextError::Write(source) => io_failure(source),
        PlaintextError::WriteZero | PlaintextError::Frame(_) | PlaintextError::WriteProgress(_) => {
            TransportFailure::Other
        }
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
