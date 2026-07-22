//! Nonblocking TCP connect phase and Mio source delegation.

use std::{io, io::Read, io::Write, net::SocketAddr};

use mio::{Interest, Registry, Token, event::Source, net::TcpStream};

use super::ConnectProgress;

/// One nonblocking TCP stream whose connect completion is explicit state.
#[derive(Debug)]
pub(in crate::reactor) struct TcpSocket {
    stream: TcpStream,
    phase: SocketPhase,
}

impl TcpSocket {
    pub(in crate::reactor) fn connect(address: SocketAddr) -> io::Result<Self> {
        TcpStream::connect(address).map(|stream| Self {
            stream,
            phase: SocketPhase::Connecting,
        })
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn open(stream: TcpStream) -> Self {
        Self {
            stream,
            phase: SocketPhase::Open,
        }
    }

    pub(in crate::reactor) fn finish_connect(&mut self) -> io::Result<ConnectProgress> {
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

impl Read for TcpSocket {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buffer)
    }
}

impl Write for TcpSocket {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.stream.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

impl Source for TcpSocket {
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
