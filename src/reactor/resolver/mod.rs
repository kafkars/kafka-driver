//! Bounded blocking DNS worker isolated from the owning I/O shard.

mod address;
mod error;
mod handle;
mod identity;
mod worker;

#[cfg(test)]
mod address_test;
#[cfg(test)]
mod queue_test;
#[cfg(test)]
mod worker_test;

pub(in crate::reactor) use address::socket_address;
pub(in crate::reactor) use error::ResolverSubmitError;
pub(in crate::reactor) use handle::Resolver;
pub(in crate::reactor) use identity::ResolverEffectIds;
