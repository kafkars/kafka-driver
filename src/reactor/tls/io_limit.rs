//! Per-call encrypted write cap presented to rustls as an ordinary writer.

use std::io::{self, Write};

use crate::reactor::tcp::TcpSocket;

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
