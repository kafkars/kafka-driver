//! Curated public vocabulary independent of hosting mode.

mod build;
mod builder;
mod call;
mod driver;
mod identity;
mod invalidation;
mod invalidation_submission;
mod observation;
mod protocol;
mod request_options;
mod route;
mod submission;
mod topic_view;
mod topic_view_after_failure;
mod topic_view_after_failure_submission;
mod tracked;
mod traffic;

#[cfg(test)]
mod call_test;
#[cfg(test)]
mod invalidation_test;
#[cfg(test)]
mod request_options_test;
#[cfg(test)]
mod route_test;
#[cfg(test)]
mod topic_view_after_failure_test;
#[cfg(test)]
mod topic_view_test;
#[cfg(test)]
mod tracked_test;

pub use crate::completion::CompletionError;
pub use crate::response::{RequestError, ResponseCloseReason};
pub use build::DriverBuildError;
pub use builder::DriverBuilder;
pub use call::Call;
pub use driver::Driver;
pub use invalidation::InvalidationDisposition;
pub use invalidation_submission::InvalidationSubmitError;
pub use kafka_driver_core::Delivery;
pub use observation::{
    BootstrapSnapshot, BrokerLaneLoadSnapshot, BrokerLanePhase, BrokerLaneSnapshot, CallCounters,
    CallLatencySnapshot, DriverSnapshot, FailureCounters, LatencyMetric, MailboxSnapshot,
    SeedSnapshot, SnapshotError, WriteQueueSnapshot,
};
pub use protocol::RequestResponsePair;
pub use request_options::RequestOptions;
pub use route::Route;
pub use submission::SubmitError;
pub use topic_view::{AvailableTopicPartition, TopicView, TopicViewError};
pub use topic_view_after_failure_submission::TopicViewAfterFailureSubmitError;
pub use tracked::{RouteFailureToken, RouteKind, RoutedCall, RoutedOutcome};
pub use traffic::TrafficClass;

pub(crate) use identity::{CallIds, DriverIdentity};
pub(crate) use tracked::RouteFact;
