//! Generated Metadata RPC ownership above one broker and below deterministic policy.

mod error;
mod identity;
mod invalidation;
mod owner;
mod request;
mod routing;
mod waiting;

#[cfg(test)]
mod identity_test;
#[cfg(test)]
mod owner_test;
#[cfg(test)]
mod waiting_test;

pub(in crate::reactor) use error::MetadataOwnerError;
pub(in crate::reactor) use owner::MetadataOwner;
pub(in crate::reactor) use routing::PartitionWait;
pub(in crate::reactor) use waiting::PartitionWaitProgress;
