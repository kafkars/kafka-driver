//! Per-call encrypted write cap presented to rustls as an ordinary writer.

use std::io::{self, Write};

use crate::reactor::tcp::TcpSocket;

/// One shared byte-work bound across TLS wire and plaintext movement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct TlsByteBudget {
    limit: usize,
    consumed: usize,
}

impl TlsByteBudget {
    pub(super) const fn new(limit: usize) -> Self {
        Self { limit, consumed: 0 }
    }

    pub(super) const fn consumed(self) -> usize {
        self.consumed
    }

    pub(super) const fn remaining(self) -> usize {
        self.limit - self.consumed
    }

    pub(super) const fn is_exhausted(self) -> bool {
        self.consumed == self.limit
    }

    pub(super) fn record(&mut self, bytes: usize) {
        debug_assert!(bytes <= self.remaining());
        self.consumed += bytes;
    }
}

pub(super) struct LimitedWriter<'a> {
    socket: &'a mut TcpSocket,
    remaining: usize,
}

impl<'a> LimitedWriter<'a> {
    pub(super) const fn new(socket: &'a mut TcpSocket, remaining: usize) -> Self {
        Self { socket, remaining }
    }
}

impl Write for LimitedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        let offered = bytes.len().min(self.remaining);
        let written = self.socket.write(&bytes[..offered])?;
        self.remaining -= written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.socket.flush()
    }
}
