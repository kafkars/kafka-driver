//! Bounded sanitized transition history for deterministic diagnostics.

use std::collections::{VecDeque, vec_deque};

use crate::ConnectionEpoch;

use super::{ConnectionInputKind, ConnectionPhase};

/// Monotonic machine-local transition identity.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TransitionSequence(u64);

impl TransitionSequence {
    /// Returns the machine-local numeric sequence.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// How an input affected current connection state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransitionDisposition {
    /// The input advanced policy or owned resources.
    Applied,
    /// Current policy rejected an internal request with explicit effects.
    Rejected,
    /// The input was current but required no additional work.
    Ignored,
    /// An external result named an obsolete epoch, resource, effect, or timer.
    IgnoredStale,
    /// The broker or transport violated a connection invariant.
    Fault,
}

/// Sanitized record of one applied machine input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransitionRecord {
    sequence: TransitionSequence,
    epoch: ConnectionEpoch,
    from: ConnectionPhase,
    input: ConnectionInputKind,
    to: ConnectionPhase,
    disposition: TransitionDisposition,
    effect_count: usize,
}

impl TransitionRecord {
    pub(super) const fn new(
        sequence: u64,
        epoch: ConnectionEpoch,
        from: ConnectionPhase,
        input: ConnectionInputKind,
        to: ConnectionPhase,
        disposition: TransitionDisposition,
        effect_count: usize,
    ) -> Self {
        Self {
            sequence: TransitionSequence(sequence),
            epoch,
            from,
            input,
            to,
            disposition,
            effect_count,
        }
    }

    /// Returns the machine-local transition sequence.
    pub const fn sequence(self) -> TransitionSequence {
        self.sequence
    }

    /// Returns the connection epoch that owned this transition.
    pub const fn epoch(self) -> ConnectionEpoch {
        self.epoch
    }

    /// Returns the lifecycle phase before the input.
    pub const fn from(self) -> ConnectionPhase {
        self.from
    }

    /// Returns the sanitized input name.
    pub const fn input(self) -> ConnectionInputKind {
        self.input
    }

    /// Returns the lifecycle phase after the input.
    pub const fn to(self) -> ConnectionPhase {
        self.to
    }

    /// Returns how the machine treated the input.
    pub const fn disposition(self) -> TransitionDisposition {
        self.disposition
    }

    /// Returns the number of effects emitted by the transition.
    pub const fn effect_count(self) -> usize {
        self.effect_count
    }
}

pub(super) struct TransitionTrace {
    capacity: usize,
    records: VecDeque<TransitionRecord>,
}

impl TransitionTrace {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            records: VecDeque::with_capacity(capacity),
        }
    }

    pub(super) fn push(&mut self, record: TransitionRecord) {
        if self.capacity == 0 {
            return;
        }
        if self.records.len() == self.capacity {
            self.records.pop_front();
        }
        self.records.push_back(record);
    }

    pub(super) fn iter(&self) -> vec_deque::Iter<'_, TransitionRecord> {
        self.records.iter()
    }
}
