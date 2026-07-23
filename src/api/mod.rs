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
mod submission;
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
pub use driver::Driver;
pub use invalidation::InvalidationDisposition;
pub use kafka_driver_core::Delivery;
pub use observation::{
    BootstrapSnapshot, BrokerLaneLoadSnapshot, BrokerLanePhase, BrokerLaneSnapshot, CallCounters,
    CallLatencySnapshot, DriverSnapshot, FailureCounters, LatencyMetric, MailboxSnapshot,
    SeedSnapshot, SnapshotError, WriteQueueSnapshot,
};
pub use protocol::RequestResponsePair;
pub use route::Route;
pub use submission::SubmitError;
pub use tracked::{RouteReceipt, RoutedCall, RoutedOutcome};
pub use traffic::TrafficClass;

pub(crate) use identity::CallIds;
