//! Typed identities that make stale external work rejectable as data.

mod call;
mod effect;
mod transport;

pub use call::{CallId, OperationId};
pub use effect::{EffectId, TimerId};
pub use transport::{ConnectionEpoch, TransportId};
