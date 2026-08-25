//! Dedicated-thread ownership over the same public embedded reactor.

mod error;
mod local;
mod owner;

#[cfg(test)]
mod local_test;

pub use error::DriverHostError;
pub use owner::DriverHost;
