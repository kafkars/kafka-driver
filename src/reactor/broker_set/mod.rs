//! Shard-local ownership of the seed and bounded broker-child namespace.

mod child;
mod child_io;
mod child_resolution;
mod error;
mod io;
mod lane;
mod owner;
mod proof;
mod replacement;
mod routing;
mod seed;
mod waiting;

#[cfg(test)]
mod address_refresh_test;
#[cfg(test)]
mod deadline_test;
#[cfg(test)]
mod lane_test;
#[cfg(test)]
mod owner_test;
#[cfg(test)]
mod routing_test;
#[cfg(test)]
mod terminal_test;
#[cfg(test)]
mod waiting_test;

pub(in crate::reactor) use error::BrokerSetError;
pub(in crate::reactor) use lane::BrokerLane;
pub(in crate::reactor) use owner::BrokerSet;
