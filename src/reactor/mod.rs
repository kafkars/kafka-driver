//! Single-owner host, bounded command mailbox, and cross-thread wake contract.

mod address_rotation;
mod backend;
mod bootstrap;
#[allow(
    dead_code,
    unused_imports,
    reason = "private adapters are activated incrementally by the Bornera cutover"
)]
mod bornera;
mod broker;
mod broker_rpc;
mod broker_set;
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
mod plaintext;
mod poller;
mod resolver;
mod resource;
mod scram_proof;
mod tcp;
mod timer;
#[cfg(feature = "tls-rustls")]
mod tls;
mod transport;
mod wait_queue;
mod waiter;
mod wake;

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
mod wait_queue_test;

pub(in crate::reactor) use backend::{LegacyBackend, ReactorBackend};
pub(in crate::reactor) use broker_rpc::{BrokerRpc, BrokerRpcError, LegacyBrokerRpc};
pub(crate) use clock::ReactorClock;
pub(crate) use command::Command;
pub use error::ReactorError;
pub use host::{Reactor, TurnOutcome};
pub(in crate::reactor) use invalidation::{InvalidationSubscribers, RouteInvalidation};
pub(crate) use mailbox::{MailboxSender, TrySendError, mailbox};
pub(in crate::reactor) use poller::{PollEvent, PollInterest, Poller, Readiness};
pub(crate) use waiter::DriverWaiter;
pub use wake::WakeHandle;
