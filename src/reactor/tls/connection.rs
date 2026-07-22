//! TLS session, TCP capability, framing, and ordered request-byte ownership.

use std::{io, net::SocketAddr};

use kafka_driver_core::{CallId, EffectId};
use kafka_driver_transport::{FrameDecoder, WriteAccepted, WriteAdmissionError, WriteQueue};
use kafka_wire_core::Bytes;
use mio::{Interest, Registry, Token, event::Source};

use crate::{
    config::TlsConnectionConfig,
    reactor::{
        tcp::{ConnectProgress, TcpSocket},
        transport::TransportLimits,
    },
};

use super::{TlsConnectError, TlsError};

#[derive(Debug)]
pub(in crate::reactor) struct TlsConnection {
    pub(super) socket: TcpSocket,
    pub(super) tls: rustls::ClientConnection,
    pub(super) frames: FrameDecoder,
    pub(super) writes: WriteQueue,
    pub(super) read_buffer: Box<[u8]>,
    pub(super) max_buffered_read_bytes: usize,
}

impl TlsConnection {
    pub(in crate::reactor) fn connect(
        address: SocketAddr,
        limits: TransportLimits,
        config: &TlsConnectionConfig,
    ) -> Result<Self, TlsConnectError> {
        let mut tls = config
            .start_connection()
            .map_err(TlsConnectError::Session)?;
        tls.set_buffer_limit(Some(limits.write().max_buffered_bytes()));
        let socket = TcpSocket::connect(address).map_err(TlsConnectError::Tcp)?;
        Ok(Self {
            socket,
            tls,
            frames: FrameDecoder::new(limits.frame()),
            writes: WriteQueue::new(limits.write()),
            read_buffer: vec![0; limits.read_chunk_bytes().get()].into_boxed_slice(),
            max_buffered_read_bytes: limits.frame().max_buffered_bytes(),
        })
    }

    pub(in crate::reactor) fn finish_connect(&mut self) -> Result<ConnectProgress, TlsError> {
        self.socket.finish_connect().map_err(TlsError::TcpConnect)
    }

    pub(in crate::reactor) fn admit_write(
        &mut self,
        call_id: CallId,
        effect_id: EffectId,
        frame: Bytes,
    ) -> Result<WriteAccepted, WriteAdmissionError> {
        self.writes.admit(call_id, effect_id, frame)
    }

    #[cfg(test)]
    pub(in crate::reactor) fn queued_write_frames(&self) -> usize {
        self.writes.queued_frames()
    }
}

impl Source for TlsConnection {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        self.socket.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        self.socket.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        self.socket.deregister(registry)
    }
}
