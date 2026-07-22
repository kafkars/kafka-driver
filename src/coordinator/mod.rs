//! Generated `FindCoordinator` adaptation around deterministic coordinator policy.

mod error;
mod request;
mod response;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;

pub(crate) use error::CoordinatorBuildError;
pub(crate) use request::find_coordinator_request;
pub(crate) use response::coordinator_target;
