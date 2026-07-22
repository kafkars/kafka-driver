//! Curated public vocabulary independent of hosting mode.

mod build;
mod call;
mod driver;
mod identity;
mod protocol;
mod route;
mod traffic;

#[cfg(test)]
mod route_test;

pub use crate::completion::{CancellationRequest, CompletionError};
pub use crate::response::{RequestError, ResponseCloseReason};
pub use build::DriverBuildError;
pub use call::Call;
pub use driver::{Driver, DriverBuilder, SubmitError};
pub use kafka_driver_core::Delivery;
pub use protocol::RequestResponsePair;
pub use route::Route;
pub use traffic::TrafficClass;

pub(crate) use identity::CallIds;
