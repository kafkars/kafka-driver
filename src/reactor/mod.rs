//! Single-owner host, bounded command mailbox, and cross-thread wake contract.

mod address_rotation;
mod backend;
mod bootstrap;
mod bornera;
mod broker;
mod broker_lane;
mod broker_rpc;
mod causality;
mod clock;
mod command;
mod coordinator;
mod direct_plaintext;
mod entropy;
mod error;
mod host;
mod invalidation;
mod mailbox;
mod metadata;
mod resolver;
mod route_waiting;
mod scram_proof;
mod transport;
mod wait_queue;
mod waiter;
mod wake;
#[cfg(test)]
mod wake_fixture_test;
mod worker_shutdown;

#[cfg(test)]
mod address_rotation_test;
#[cfg(test)]
mod broker_rpc_test;
#[cfg(test)]
mod causality_test;
#[cfg(test)]
mod clock_test;
#[cfg(test)]
mod command_test;
#[cfg(test)]
mod entropy_test;
#[cfg(test)]
mod invalidation_test;
#[cfg(test)]
mod mailbox_test;
#[cfg(test)]
mod route_waiting_test;
#[cfg(test)]
mod wait_queue_test;

pub(in crate::reactor) use backend::{BackendRpcAccessError, ReactorBackend};
pub(in crate::reactor) use broker_lane::BrokerLane;
pub(in crate::reactor) use broker_rpc::{BrokerRpc, BrokerRpcError};
pub(crate) use clock::ReactorClock;
pub(crate) use command::Command;
pub use error::ReactorError;
pub use host::{Reactor, TurnOutcome};
pub(in crate::reactor) use invalidation::{InvalidationSubscribers, RouteInvalidation};
pub(crate) use mailbox::{MailboxSender, TrySendError, mailbox};
pub(crate) use waiter::DriverWaiter;
pub use wake::WakeHandle;
