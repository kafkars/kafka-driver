//! Curated public vocabulary independent of hosting mode.

mod call;
mod driver;
mod protocol;
mod traffic;

pub use crate::completion::{CancellationRequest, CompletionError};
pub use call::Call;
pub use driver::{Driver, DriverBuilder, SubmitError};
pub use kafka_driver_core::Delivery;
pub use protocol::RequestResponsePair;
pub use traffic::TrafficClass;
