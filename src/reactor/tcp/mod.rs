//! Nonblocking TCP socket capability shared by plaintext and secured transports.

mod progress;
mod socket;

pub(in crate::reactor) use progress::ConnectProgress;
pub(in crate::reactor) use socket::TcpSocket;
