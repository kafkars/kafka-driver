//! Shard-local ownership of the seed and bounded broker-child namespace.

mod error;
mod owner;

#[cfg(test)]
mod owner_test;

pub(in crate::reactor) use error::BrokerSetError;
pub(in crate::reactor) use owner::BrokerSet;
