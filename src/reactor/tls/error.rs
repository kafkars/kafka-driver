//! TLS construction and bounded byte-progress failures.

use std::{fmt, io};

use kafka_driver_transport::{FrameDecodeError, WriteProgressError};

#[derive(Debug)]
pub(in crate::reactor) enum TlsConnectError {
    Tcp(io::Error),
    Session(rustls::Error),
}

impl fmt::Display for TlsConnectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(_) => formatter.write_str("TLS TCP connect creation failed"),
            Self::Session(_) => formatter.write_str("rustls client session creation failed"),
        }
    }
}

impl std::error::Error for TlsConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Tcp(source) => Some(source),
            Self::Session(source) => Some(source),
        }
    }
}

#[derive(Debug)]
pub(in crate::reactor) enum TlsError {
    TcpConnect(io::Error),
    TlsRead(io::Error),
    TlsWrite(io::Error),
    PlaintextRead(io::Error),
    PlaintextWrite(io::Error),
    Protocol(rustls::Error),
    WriteZero,
    Frame(FrameDecodeError),
    WriteProgress(WriteProgressError),
}

impl fmt::Display for TlsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TcpConnect(_) => formatter.write_str("TLS TCP connect verification failed"),
            Self::TlsRead(_) => formatter.write_str("encrypted socket read failed"),
            Self::TlsWrite(_) => formatter.write_str("encrypted socket write failed"),
            Self::PlaintextRead(_) => formatter.write_str("rustls plaintext read failed"),
            Self::PlaintextWrite(_) => formatter.write_str("rustls plaintext write failed"),
            Self::Protocol(_) => formatter.write_str("TLS protocol verification failed"),
            Self::WriteZero => formatter.write_str("TLS socket write made zero progress"),
            Self::Frame(error) => error.fmt(formatter),
            Self::WriteProgress(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for TlsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TcpConnect(source)
            | Self::TlsRead(source)
            | Self::TlsWrite(source)
            | Self::PlaintextRead(source)
            | Self::PlaintextWrite(source) => Some(source),
            Self::Protocol(source) => Some(source),
            Self::Frame(source) => Some(source),
            Self::WriteProgress(source) => Some(source),
            Self::WriteZero => None,
        }
    }
}

impl From<FrameDecodeError> for TlsError {
    fn from(source: FrameDecodeError) -> Self {
        Self::Frame(source)
    }
}

impl From<WriteProgressError> for TlsError {
    fn from(source: WriteProgressError) -> Self {
        Self::WriteProgress(source)
    }
}
