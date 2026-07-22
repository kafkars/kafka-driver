//! Nonblocking TCP connect phase and Mio source delegation.

use std::{io, io::Read, io::Write, net::SocketAddr};

use mio::{Interest, Registry, Token, event::Source, net::TcpStream};

use super::ConnectProgress;

/// One TCP stream whose connect completion is explicit state.
#[derive(Debug)]
pub(super) struct PlaintextSocket {
    stream: TcpStream,
    phase: SocketPhase,
}

impl PlaintextSocket {
    pub(super) fn connect(address: SocketAddr) -> io::Result<Self> {
        TcpStream::connect(address).map(|stream| Self {
            stream,
            phase: SocketPhase::Connecting,
        })
    }

    #[cfg(test)]
    pub(super) const fn open(stream: TcpStream) -> Self {
        Self {
            stream,
            phase: SocketPhase::Open,
        }
    }

    pub(super) fn finish_connect(&mut self) -> io::Result<ConnectProgress> {
        if self.phase == SocketPhase::Open {
            return Ok(ConnectProgress::AlreadyOpen);
        }
        if let Some(source) = self.stream.take_error()? {
            return Err(source);
        }
        match self.stream.peer_addr() {
            Ok(_) => {
                self.phase = SocketPhase::Open;
                Ok(ConnectProgress::Opened)
            }
            Err(source) if source.kind() == io::ErrorKind::NotConnected => {
                Ok(ConnectProgress::Pending)
            }
            Err(source) => Err(source),
        }
    }
}

impl Read for PlaintextSocket {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buffer)
    }
}

impl Write for PlaintextSocket {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.stream.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

impl Source for PlaintextSocket {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        self.stream.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        self.stream.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        self.stream.deregister(registry)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SocketPhase {
    Connecting,
    Open,
}
