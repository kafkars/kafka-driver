//! One nonblocking TCP stream composed with bounded frame and write ownership.

use std::{io, io::Read, io::Write, net::SocketAddr};

use kafka_driver_core::{CallId, EffectId};
use kafka_driver_transport::{
    FrameBody, FrameDecoder, WriteAccepted, WriteAdmissionError, WriteProgress, WriteQueue,
};
use kafka_wire_core::Bytes;
#[cfg(test)]
use mio::net::TcpStream;
use mio::{Registry, Token, event::Source};

use super::{
    CompletedWrite, PlaintextError, PlaintextLimits, ReadBudget, ReadProgress, ReadState,
    WriteBudget, WriteDrive, WriteState,
};
use crate::reactor::tcp::{ConnectProgress, TcpSocket};

/// One reactor-owned plaintext socket and its ordered byte progress.
#[derive(Debug)]
pub(in crate::reactor) struct PlaintextConnection {
    socket: TcpSocket,
    frames: FrameDecoder,
    writes: WriteQueue,
    read_buffer: Box<[u8]>,
    max_buffered_read_bytes: usize,
}

impl PlaintextConnection {
    pub(in crate::reactor) fn connect(
        address: SocketAddr,
        limits: PlaintextLimits,
    ) -> io::Result<Self> {
        TcpSocket::connect(address).map(|socket| Self::with_socket(socket, limits))
    }

    #[cfg(test)]
    pub(in crate::reactor) fn new(socket: TcpStream, limits: PlaintextLimits) -> Self {
        Self::with_socket(TcpSocket::open(socket), limits)
    }

    fn with_socket(socket: TcpSocket, limits: PlaintextLimits) -> Self {
        Self {
            socket,
            frames: FrameDecoder::new(limits.frame()),
            writes: WriteQueue::new(limits.write()),
            read_buffer: vec![0; limits.read_chunk_bytes().get()].into_boxed_slice(),
            max_buffered_read_bytes: limits.frame().max_buffered_bytes(),
        }
    }

    pub(in crate::reactor) fn finish_connect(&mut self) -> Result<ConnectProgress, PlaintextError> {
        self.socket
            .finish_connect()
            .map_err(PlaintextError::Connect)
    }

    pub(in crate::reactor) fn admit_write(
        &mut self,
        call_id: CallId,
        effect_id: EffectId,
        frame: Bytes,
    ) -> Result<WriteAccepted, WriteAdmissionError> {
        self.writes.admit(call_id, effect_id, frame)
    }

    pub(in crate::reactor) fn drive_read(
        &mut self,
        budget: ReadBudget,
        destination: &mut Vec<FrameBody>,
    ) -> Result<ReadProgress, PlaintextError> {
        let mut bytes = 0;
        let mut frames = 0;
        loop {
            while frames < budget.frames() {
                let Some(frame) = self.frames.next_frame()? else {
                    break;
                };
                destination.push(frame);
                frames += 1;
            }
            if frames == budget.frames() || bytes == budget.bytes() {
                return Ok(ReadProgress::new(bytes, frames, ReadState::BudgetExhausted));
            }

            let decoder_space = self
                .max_buffered_read_bytes
                .saturating_sub(self.frames.buffered_bytes());
            let read_bytes = self
                .read_buffer
                .len()
                .min(budget.bytes() - bytes)
                .min(decoder_space);
            if read_bytes == 0 {
                return Ok(ReadProgress::new(bytes, frames, ReadState::BudgetExhausted));
            }
            match self.socket.read(&mut self.read_buffer[..read_bytes]) {
                Ok(0) => {
                    return Ok(ReadProgress::new(bytes, frames, ReadState::PeerClosed));
                }
                Ok(read) => {
                    self.frames.feed(&self.read_buffer[..read])?;
                    bytes += read;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(ReadProgress::new(bytes, frames, ReadState::Blocked));
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                    return Ok(ReadProgress::new(bytes, frames, ReadState::Interrupted));
                }
                Err(error) => return Err(PlaintextError::Read(error)),
            }
        }
    }

    pub(in crate::reactor) fn drive_write(
        &mut self,
        budget: WriteBudget,
        destination: &mut Vec<CompletedWrite>,
    ) -> Result<WriteDrive, PlaintextError> {
        let mut bytes = 0;
        let mut completed = 0;
        loop {
            if self.writes.queued_frames() == 0 {
                return Ok(WriteDrive::new(bytes, completed, WriteState::Idle));
            }
            if bytes == budget.bytes() {
                return Ok(WriteDrive::new(
                    bytes,
                    completed,
                    WriteState::BudgetExhausted,
                ));
            }
            let Some(remaining_budget) = std::num::NonZeroUsize::new(budget.bytes() - bytes) else {
                continue;
            };
            let Some(front) = self.writes.front(remaining_budget) else {
                return Ok(WriteDrive::new(bytes, completed, WriteState::Idle));
            };
            match self.socket.write(front.bytes()) {
                Ok(0) => return Err(PlaintextError::WriteZero),
                Ok(written) => {
                    bytes += written;
                    if let WriteProgress::Complete {
                        call_id,
                        effect_id,
                        frame_bytes,
                    } = self.writes.advance(front.effect_id(), written)?
                    {
                        destination.push(CompletedWrite::new(call_id, effect_id, frame_bytes));
                        completed += 1;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(WriteDrive::new(bytes, completed, WriteState::Blocked));
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                    return Ok(WriteDrive::new(bytes, completed, WriteState::Interrupted));
                }
                Err(error) => return Err(PlaintextError::Write(error)),
            }
        }
    }

    #[cfg(test)]
    pub(in crate::reactor) fn queued_write_frames(&self) -> usize {
        self.writes.queued_frames()
    }
}

impl Source for PlaintextConnection {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.socket.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.socket.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        self.socket.deregister(registry)
    }
}
