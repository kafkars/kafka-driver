//! Sans-I/O Kafka framing and ordered byte movement below driver policy.
//!
//! This published support crate owns bounded transport primitives without
//! sockets, pollers, threads, real clocks, or connection policy. `kafka-driver`
//! remains the primary public RPC API.

mod frame;
mod write;

pub use frame::{FrameBody, FrameDecodeError, FrameDecoder, FrameLimits, FrameLimitsError};
pub use write::{
    DiscardedWrites, WriteAccepted, WriteAdmissionError, WriteAdmissionFailure, WriteIdentityKind,
    WriteProgress, WriteProgressError, WriteQueue, WriteQueueLimits, WriteSlice,
};
