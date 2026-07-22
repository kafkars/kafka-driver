//! Dedicated-thread ownership over the same public embedded reactor.

mod error;
mod owner;

pub use error::DriverHostError;
pub use owner::DriverHost;
