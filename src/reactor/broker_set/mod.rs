//! Shard-local ownership of the seed and bounded broker-child namespace.

mod capacity;
mod child;
mod child_io;
mod child_resolution;
mod deadline_index;
mod error;
mod io;
mod lane_queue;
mod observation;
mod owner;
mod proof;
mod replacement;
mod routing;
mod runnable;
mod scheduling;
mod seed;
mod slots;

#[cfg(test)]
mod address_refresh_test;
#[cfg(test)]
mod deadline_budget_test;
#[cfg(test)]
mod deadline_index_test;
#[cfg(test)]
mod deadline_test;
#[cfg(test)]
mod lane_queue_test;
#[cfg(test)]
mod lane_test;
#[cfg(test)]
mod owner_test;
#[cfg(test)]
mod replacement_test;
#[cfg(test)]
mod resolution_permit_test;
#[cfg(test)]
mod routing_test;
#[cfg(test)]
mod seed_refresh_test;
#[cfg(test)]
mod seed_terminal_test;
#[cfg(test)]
mod seed_waiting_test;
#[cfg(test)]
mod slots_test;
#[cfg(test)]
mod terminal_test;
#[cfg(test)]
mod waiting_test;

pub(in crate::reactor) use super::broker_lane::BrokerLane;
pub(in crate::reactor) use error::BrokerSetError;
pub(in crate::reactor) use owner::BrokerSet;
