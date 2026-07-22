//! Persistent resource bounds and per-turn budgets for one broker owner.

use std::num::NonZeroUsize;

use kafka_driver_core::ConnectionLimits;

use crate::reactor::plaintext::{PlaintextLimits, ReadBudget, WriteBudget};

const CONNECTION_CAPACITY: NonZeroUsize = nonzero(256);
const RESOURCE_CAPACITY: NonZeroUsize = nonzero(1);
const TIMER_BUDGET: NonZeroUsize = nonzero(256);
const READ_BYTES: NonZeroUsize = nonzero(1024 * 1024);
const READ_FRAMES: NonZeroUsize = nonzero(64);
const WRITE_BYTES: NonZeroUsize = nonzero(1024 * 1024);

/// Coherent limits shared by machine, response, timer, and transport ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct BrokerLimits {
    connection: ConnectionLimits,
    response_capacity: NonZeroUsize,
    resource_capacity: NonZeroUsize,
    timer_capacity: NonZeroUsize,
    timer_budget: NonZeroUsize,
    plaintext: PlaintextLimits,
    read_budget: ReadBudget,
    write_budget: WriteBudget,
}

impl BrokerLimits {
    pub(in crate::reactor) const fn connection(self) -> ConnectionLimits {
        self.connection
    }

    pub(in crate::reactor) const fn response_capacity(self) -> NonZeroUsize {
        self.response_capacity
    }

    pub(in crate::reactor) const fn resource_capacity(self) -> NonZeroUsize {
        self.resource_capacity
    }

    pub(in crate::reactor) const fn timer_capacity(self) -> NonZeroUsize {
        self.timer_capacity
    }

    pub(in crate::reactor) const fn timer_budget(self) -> NonZeroUsize {
        self.timer_budget
    }

    pub(in crate::reactor) const fn plaintext(self) -> PlaintextLimits {
        self.plaintext
    }

    pub(in crate::reactor) const fn read_budget(self) -> ReadBudget {
        self.read_budget
    }

    pub(in crate::reactor) const fn write_budget(self) -> WriteBudget {
        self.write_budget
    }
}

impl Default for BrokerLimits {
    fn default() -> Self {
        let connection = ConnectionLimits::new(CONNECTION_CAPACITY, 128);
        Self {
            connection,
            response_capacity: connection.max_in_flight(),
            resource_capacity: RESOURCE_CAPACITY,
            timer_capacity: connection.max_in_flight(),
            timer_budget: TIMER_BUDGET,
            plaintext: PlaintextLimits::default(),
            read_budget: ReadBudget::new(READ_BYTES, READ_FRAMES),
            write_budget: WriteBudget::new(WRITE_BYTES),
        }
    }
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("broker defaults must be nonzero");
    };
    value
}
