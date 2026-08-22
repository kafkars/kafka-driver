//! Bounded generated discovery and public-call ownership for coordinator routes.

mod admission;
mod drive;
mod drive_retry;
mod effect;
mod entry;
mod error;
mod identity;
mod invalidation;
mod invalidation_wait;
mod owner;
mod routing;
mod waiting;
mod waiting_progress;

#[cfg(test)]
mod owner_test;
#[cfg(test)]
mod waiting_test;

pub(super) use effect::CoordinatorStep;
pub(in crate::reactor) use error::CoordinatorOwnerError;
pub(in crate::reactor) use owner::CoordinatorOwner;
pub(in crate::reactor) use waiting::CoordinatorWait;
