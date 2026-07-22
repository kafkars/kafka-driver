//! Curated public vocabulary independent of hosting mode.

mod build;
mod builder;
mod call;
mod driver;
mod identity;
mod invalidation;
mod observation;
mod protocol;
mod route;
mod tracked;
mod traffic;

#[cfg(test)]
mod route_test;
#[cfg(test)]
mod tracked_test;

pub use crate::completion::CompletionError;
pub use crate::response::{RequestError, ResponseCloseReason};
pub use build::DriverBuildError;
pub use builder::DriverBuilder;
pub use call::Call;
pub use driver::{Driver, SubmitError};
pub use invalidation::InvalidationDisposition;
pub use kafka_driver_core::Delivery;
pub use observation::{
    BrokerLanePhase, BrokerLaneSnapshot, DriverSnapshot, MailboxSnapshot, SeedSnapshot,
    SnapshotError,
};
pub use protocol::RequestResponsePair;
pub use route::Route;
pub use tracked::{RouteReceipt, RoutedCall, RoutedOutcome};
pub use traffic::TrafficClass;

pub(crate) use identity::CallIds;
pub(crate) use tracked::{RouteReceiptWriter, route_receipt_pair};
