//! Reactor interpretation of deterministic bootstrap DNS effects.

mod error;
mod owner;

#[cfg(test)]
mod owner_test;

pub(in crate::reactor) use error::BootstrapOwnerError;
pub(in crate::reactor) use owner::{BootstrapAction, BootstrapOwner, ResolvedSeed};
