//! Current seed and sparse discovered-broker lane ownership.

use kafka_driver_core::{BrokerId, BrokerState, CloseReason, ConnectionPhase, DnsFailure};

use crate::TrafficClass;

/// Current lifecycle of the configured seed connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SeedSnapshot {
    broker_state: BrokerState,
    connection_phase: ConnectionPhase,
    last_close_reason: Option<CloseReason>,
    write_queue: WriteQueueSnapshot,
}

impl SeedSnapshot {
    pub(crate) const fn new(
        broker_state: BrokerState,
        connection_phase: ConnectionPhase,
        last_close_reason: Option<CloseReason>,
        write_queue: WriteQueueSnapshot,
    ) -> Self {
        Self {
            broker_state,
            connection_phase,
            last_close_reason,
            write_queue,
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

    /// Returns the sanitized reason the previous connection epoch ended.
    pub const fn last_close_reason(self) -> Option<CloseReason> {
        self.last_close_reason
    }

    /// Returns current retained writes and cumulative saturation totals.
    pub const fn write_queue(self) -> WriteQueueSnapshot {
        self.write_queue
    }
}

/// One live discovered broker and semantic traffic lane.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrokerLaneSnapshot {
    broker_id: BrokerId,
    traffic_class: TrafficClass,
    phase: BrokerLanePhase,
    last_dns_failure: Option<DnsFailure>,
    last_close_reason: Option<CloseReason>,
    load: BrokerLaneLoadSnapshot,
}

impl BrokerLaneSnapshot {
    pub(crate) const fn new(
        broker_id: BrokerId,
        traffic_class: TrafficClass,
        phase: BrokerLanePhase,
        last_dns_failure: Option<DnsFailure>,
        last_close_reason: Option<CloseReason>,
        load: BrokerLaneLoadSnapshot,
    ) -> Self {
        Self {
            broker_id,
            traffic_class,
            phase,
            last_dns_failure,
            last_close_reason,
            load,
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

    /// Returns the last sanitized advertised-name resolution failure.
    pub const fn last_dns_failure(self) -> Option<DnsFailure> {
        self.last_dns_failure
    }

    /// Returns the sanitized reason the previous connection epoch ended.
    pub const fn last_close_reason(self) -> Option<CloseReason> {
        self.last_close_reason
    }

    /// Returns current waiting and writer load for this sparse lane.
    pub const fn load(self) -> BrokerLaneLoadSnapshot {
        self.load
    }

    /// Returns calls retained while this lane cannot admit them.
    pub const fn waiting_calls(self) -> usize {
        self.load.waiting_calls()
    }

    /// Returns encoded-work bytes retained by those waiting calls.
    pub const fn waiting_bytes(self) -> usize {
        self.load.waiting_bytes()
    }

    /// Returns current retained writes and cumulative saturation totals.
    pub const fn write_queue(self) -> WriteQueueSnapshot {
        self.load.write_queue()
    }
}

/// Current waiting-call and writer load for one sparse broker lane.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BrokerLaneLoadSnapshot {
    waiting_calls: usize,
    waiting_bytes: usize,
    write_queue: WriteQueueSnapshot,
}

impl BrokerLaneLoadSnapshot {
    pub(crate) const fn new(
        waiting_calls: usize,
        waiting_bytes: usize,
        write_queue: WriteQueueSnapshot,
    ) -> Self {
        Self {
            waiting_calls,
            waiting_bytes,
            write_queue,
        }
    }

    /// Returns calls retained while this lane cannot admit them.
    pub const fn waiting_calls(self) -> usize {
        self.waiting_calls
    }

    /// Returns encoded-work bytes retained by those waiting calls.
    pub const fn waiting_bytes(self) -> usize {
        self.waiting_bytes
    }

    /// Returns current retained writes and cumulative saturation totals.
    pub const fn write_queue(self) -> WriteQueueSnapshot {
        self.write_queue
    }
}

/// Current ordered-writer retention plus cumulative capacity rejections.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteQueueSnapshot {
    queued_frames: usize,
    retained_bytes: usize,
    frame_capacity_rejections: u64,
    byte_capacity_rejections: u64,
}

impl WriteQueueSnapshot {
    pub(crate) const fn new(
        queued_frames: usize,
        retained_bytes: usize,
        frame_capacity_rejections: u64,
        byte_capacity_rejections: u64,
    ) -> Self {
        Self {
            queued_frames,
            retained_bytes,
            frame_capacity_rejections,
            byte_capacity_rejections,
        }
    }

    /// Returns complete encoded frames retained in FIFO order.
    pub const fn queued_frames(self) -> usize {
        self.queued_frames
    }

    /// Returns original encoded bytes retained by queued frames.
    pub const fn retained_bytes(self) -> usize {
        self.retained_bytes
    }

    /// Returns admissions rejected at the queued-frame count bound.
    pub const fn frame_capacity_rejections(self) -> u64 {
        self.frame_capacity_rejections
    }

    /// Returns admissions rejected at the retained-byte bound.
    pub const fn byte_capacity_rejections(self) -> u64 {
        self.byte_capacity_rejections
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
