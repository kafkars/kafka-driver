//! Selected broker byte stream with shared bounded progress vocabulary.

#[cfg(test)]
mod connection;
#[cfg(test)]
mod error;
mod limits;
#[cfg(test)]
mod progress;
#[cfg(test)]
mod simulated;

#[cfg(test)]
pub(in crate::reactor) use connection::TransportConnection;
#[cfg(test)]
pub(in crate::reactor) use error::{TransportConnectError, TransportError};
pub(in crate::reactor) use limits::TransportLimits;
pub(in crate::reactor) use limits::{ReadBudget, WriteBudget};
#[cfg(test)]
pub(in crate::reactor) use progress::{
    CompletedWrite, ReadProgress, ReadState, WriteDrive, WriteState,
};
#[cfg(test)]
pub(in crate::reactor) use simulated::SimulatedConnection;
