//! Transport selection that preserves one framing and progress contract.

use std::{io, net::SocketAddr};

use kafka_driver_core::{CallId, EffectId};
use kafka_driver_transport::{FrameBody, WriteAccepted, WriteAdmissionError};
use kafka_wire_core::Bytes;
use mio::{Interest, Registry, Token, event::Source};

use crate::{
    config::BrokerSecurity,
    reactor::{plaintext::PlaintextConnection, tcp::ConnectProgress},
};

#[cfg(feature = "tls-rustls")]
use crate::reactor::tls::TlsConnection;

use super::{
    CompletedWrite, ReadBudget, ReadProgress, TransportConnectError, TransportError,
    TransportLimits, WriteBudget, WriteDrive,
};

#[derive(Debug)]
pub(in crate::reactor) enum TransportConnection {
    Plaintext(PlaintextConnection),
    #[cfg(feature = "tls-rustls")]
    Rustls(Box<TlsConnection>),
}

impl TransportConnection {
    pub(in crate::reactor) fn connect(
        address: SocketAddr,
        limits: TransportLimits,
        security: &BrokerSecurity,
    ) -> Result<Self, TransportConnectError> {
        match security {
            BrokerSecurity::Plaintext => PlaintextConnection::connect(address, limits)
                .map(Self::Plaintext)
                .map_err(TransportConnectError::Plaintext),
            #[cfg(feature = "tls-rustls")]
            BrokerSecurity::Rustls(config) => TlsConnection::connect(address, limits, config)
                .map(Box::new)
                .map(Self::Rustls)
                .map_err(TransportConnectError::Rustls),
        }
    }

    pub(in crate::reactor) fn finish_connect(&mut self) -> Result<ConnectProgress, TransportError> {
        match self {
            Self::Plaintext(connection) => connection
                .finish_connect()
                .map_err(TransportError::Plaintext),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(connection) => connection.finish_connect().map_err(TransportError::Rustls),
        }
    }

    pub(in crate::reactor) fn admit_write(
        &mut self,
        call_id: CallId,
        effect_id: EffectId,
        frame: Bytes,
    ) -> Result<WriteAccepted, WriteAdmissionError> {
        match self {
            Self::Plaintext(connection) => connection.admit_write(call_id, effect_id, frame),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(connection) => connection.admit_write(call_id, effect_id, frame),
        }
    }

    pub(in crate::reactor) fn drive_read(
        &mut self,
        budget: ReadBudget,
        destination: &mut Vec<FrameBody>,
    ) -> Result<ReadProgress, TransportError> {
        match self {
            Self::Plaintext(connection) => connection
                .drive_read(budget, destination)
                .map_err(TransportError::Plaintext),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(connection) => connection
                .drive_read(budget, destination)
                .map_err(TransportError::Rustls),
        }
    }

    pub(in crate::reactor) fn drive_write(
        &mut self,
        budget: WriteBudget,
        destination: &mut Vec<CompletedWrite>,
    ) -> Result<WriteDrive, TransportError> {
        match self {
            Self::Plaintext(connection) => connection
                .drive_write(budget, destination)
                .map_err(TransportError::Plaintext),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(connection) => connection
                .drive_write(budget, destination)
                .map_err(TransportError::Rustls),
        }
    }

    pub(in crate::reactor) fn queued_write_frames(&self) -> usize {
        match self {
            Self::Plaintext(connection) => connection.queued_write_frames(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(connection) => connection.queued_write_frames(),
        }
    }

    pub(in crate::reactor) const fn queued_write_bytes(&self) -> usize {
        match self {
            Self::Plaintext(connection) => connection.queued_write_bytes(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(connection) => connection.queued_write_bytes(),
        }
    }
}

impl Source for TransportConnection {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        match self {
            Self::Plaintext(connection) => connection.register(registry, token, interests),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(connection) => connection.register(registry, token, interests),
        }
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        match self {
            Self::Plaintext(connection) => connection.reregister(registry, token, interests),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(connection) => connection.reregister(registry, token, interests),
        }
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        match self {
            Self::Plaintext(connection) => connection.deregister(registry),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(connection) => connection.deregister(registry),
        }
    }
}
