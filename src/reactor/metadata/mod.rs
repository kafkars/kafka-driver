//! Generated Metadata RPC ownership above one broker and below deterministic policy.

mod error;
mod identity;
mod owner;

#[cfg(test)]
mod identity_test;
#[cfg(test)]
mod owner_test;

pub(in crate::reactor) use error::MetadataOwnerError;
pub(in crate::reactor) use owner::MetadataOwner;
