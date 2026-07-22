//! Rustls client stream driven directly by bounded reactor readiness.

mod connection;
mod error;
mod io_limit;
mod read;
mod write;

pub(in crate::reactor) use connection::TlsConnection;
pub(in crate::reactor) use error::{TlsConnectError, TlsError};
