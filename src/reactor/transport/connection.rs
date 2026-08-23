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

#[cfg(test)]
use crate::reactor::transport::SimulatedConnection;

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
    #[cfg(test)]
    Simulated(SimulatedConnection),
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
            #[cfg(test)]
            Self::Simulated(connection) => Ok(connection.finish_connect()),
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
            #[cfg(test)]
            Self::Simulated(connection) => connection.admit_write(call_id, effect_id, frame),
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
            #[cfg(test)]
            Self::Simulated(connection) => connection
                .drive_read(budget, destination)
                .map_err(TransportError::Plaintext),
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
            #[cfg(test)]
            Self::Simulated(connection) => connection
                .drive_write(budget, destination)
                .map_err(TransportError::Plaintext),
        }
    }

    pub(in crate::reactor) fn queued_write_frames(&self) -> usize {
        match self {
            Self::Plaintext(connection) => connection.queued_write_frames(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(connection) => connection.queued_write_frames(),
            #[cfg(test)]
            Self::Simulated(connection) => connection.queued_write_frames(),
        }
    }

    pub(in crate::reactor) const fn queued_write_bytes(&self) -> usize {
        match self {
            Self::Plaintext(connection) => connection.queued_write_bytes(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(connection) => connection.queued_write_bytes(),
            #[cfg(test)]
            Self::Simulated(connection) => connection.queued_write_bytes(),
        }
    }

    #[cfg(test)]
    pub(in crate::reactor) fn fail_read_after_frame(&mut self, kind: io::ErrorKind) {
        match self {
            Self::Plaintext(connection) => connection.fail_read_after_frame(kind),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(_) => panic!("plaintext read-failure injection requires plaintext"),
            Self::Simulated(_) => panic!("plaintext read-failure injection requires plaintext"),
        }
    }

    #[cfg(test)]
    pub(in crate::reactor) fn simulated(limits: TransportLimits) -> Self {
        Self::Simulated(SimulatedConnection::new(limits))
    }

    #[cfg(test)]
    pub(in crate::reactor) fn simulated_connect(&mut self) -> bool {
        let Self::Simulated(connection) = self else {
            return false;
        };
        connection.connect();
        true
    }

    #[cfg(test)]
    pub(in crate::reactor) fn simulated_receive(&mut self, bytes: Vec<u8>) -> bool {
        let Self::Simulated(connection) = self else {
            return false;
        };
        connection.receive(bytes);
        true
    }

    #[cfg(test)]
    pub(in crate::reactor) fn take_simulated_frames(&mut self) -> Vec<Vec<u8>> {
        let Self::Simulated(connection) = self else {
            return Vec::new();
        };
        connection.take_completed_frames()
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
            #[cfg(test)]
            Self::Simulated(_) => Ok(()),
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
            #[cfg(test)]
            Self::Simulated(_) => Ok(()),
        }
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        match self {
            Self::Plaintext(connection) => connection.deregister(registry),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(connection) => connection.deregister(registry),
            #[cfg(test)]
            Self::Simulated(_) => Ok(()),
        }
    }
}
