//! Current seed and sparse discovered-broker lane ownership.

use kafka_driver_core::{BrokerId, BrokerState, ConnectionPhase};

use crate::TrafficClass;

/// Current lifecycle of the configured seed connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SeedSnapshot {
    broker_state: BrokerState,
    connection_phase: ConnectionPhase,
}

impl SeedSnapshot {
    pub(crate) const fn new(broker_state: BrokerState, connection_phase: ConnectionPhase) -> Self {
        Self {
            broker_state,
            connection_phase,
        }
    }

    /// Returns long-lived reconnect and terminal ownership.
    pub const fn broker_state(self) -> BrokerState {
        self.broker_state
    }

    /// Returns the current socket-epoch phase.
    pub const fn connection_phase(self) -> ConnectionPhase {
        self.connection_phase
    }
}

/// One live discovered broker and semantic traffic lane.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrokerLaneSnapshot {
    broker_id: BrokerId,
    traffic_class: TrafficClass,
    phase: BrokerLanePhase,
    waiting_calls: usize,
    waiting_bytes: usize,
}

impl BrokerLaneSnapshot {
    pub(crate) const fn new(
        broker_id: BrokerId,
        traffic_class: TrafficClass,
        phase: BrokerLanePhase,
        waiting_calls: usize,
        waiting_bytes: usize,
    ) -> Self {
        Self {
            broker_id,
            traffic_class,
            phase,
            waiting_calls,
            waiting_bytes,
        }
    }

    /// Returns the Kafka broker identity owning this lane.
    pub const fn broker_id(self) -> BrokerId {
        self.broker_id
    }

    /// Returns the semantic head-of-line isolation class.
    pub const fn traffic_class(self) -> TrafficClass {
        self.traffic_class
    }

    /// Returns exact current connection ownership.
    pub const fn phase(self) -> BrokerLanePhase {
        self.phase
    }

    /// Returns calls retained while this lane cannot admit them.
    pub const fn waiting_calls(self) -> usize {
        self.waiting_calls
    }

    /// Returns encoded-work bytes retained by those waiting calls.
    pub const fn waiting_bytes(self) -> usize {
        self.waiting_bytes
    }
}

/// State-valid lifecycle of one discovered-broker traffic lane.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerLanePhase {
    /// The sparse lane exists but has not started external resolution.
    Dormant,
    /// The lane owns one identity-fenced DNS request.
    Resolving,
    /// One long-lived broker owner and socket epoch exist.
    Owned {
        /// Long-lived reconnect, drain, and terminal state.
        broker: BrokerState,
        /// Current replaceable socket-epoch phase.
        connection: ConnectionPhase,
    },
    /// Current metadata removed this lane while its old resource drains.
    Retired,
}
