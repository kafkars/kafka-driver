//! Driver-facing adapter over Calandria's Mio readiness backend.

mod selector;

#[cfg(test)]
mod poller_test;

pub(in crate::reactor) use calandria::{Interest as PollInterest, PollEvent, Readiness};
pub(in crate::reactor) use selector::Poller;
