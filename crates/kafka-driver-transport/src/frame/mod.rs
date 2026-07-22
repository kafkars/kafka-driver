//! Length-delimited Kafka response-frame accumulation and extraction.

mod body;
mod decoder;
mod error;
mod limits;

#[cfg(test)]
mod decoder_test;
#[cfg(test)]
mod fuzz_test;

pub use body::FrameBody;
pub use decoder::FrameDecoder;
pub use error::FrameDecodeError;
pub use limits::{FrameLimits, FrameLimitsError};
