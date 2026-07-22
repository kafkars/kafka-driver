//! Curated public vocabulary independent of hosting mode.

mod build;
mod builder;
mod call;
mod driver;
mod identity;
mod protocol;
mod route;
mod traffic;

#[cfg(test)]
mod route_test;

pub use crate::completion::CompletionError;
pub use crate::response::{RequestError, ResponseCloseReason};
pub use build::DriverBuildError;
pub use builder::DriverBuilder;
pub use call::Call;
pub use driver::{Driver, SubmitError};
pub use kafka_driver_core::Delivery;
pub use protocol::RequestResponsePair;
pub use route::Route;
pub use traffic::TrafficClass;

pub(crate) use identity::CallIds;
