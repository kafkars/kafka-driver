//! Shared bounded byte-stream configuration consumed by Bornera lanes.

mod limits;
pub(in crate::reactor) use limits::TransportLimits;
pub(in crate::reactor) use limits::{ReadBudget, WriteBudget};
