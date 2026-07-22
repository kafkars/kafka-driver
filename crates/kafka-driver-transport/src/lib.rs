//! Sans-I/O Kafka framing and ordered byte movement below driver policy.
//!
//! This workspace-private crate owns bounded transport primitives without
//! sockets, pollers, threads, real clocks, or connection policy.

mod frame;

pub use frame::{FrameBody, FrameDecodeError, FrameDecoder, FrameLimits, FrameLimitsError};
