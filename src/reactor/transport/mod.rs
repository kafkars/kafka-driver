//! Selected broker byte stream with shared bounded progress vocabulary.

mod connection;
mod error;
mod limits;
mod progress;

pub(in crate::reactor) use connection::TransportConnection;
pub(in crate::reactor) use error::{TransportConnectError, TransportError};
pub(in crate::reactor) use limits::{ReadBudget, TransportLimits, WriteBudget};
pub(in crate::reactor) use progress::{
    CompletedWrite, ReadProgress, ReadState, WriteDrive, WriteState,
};
