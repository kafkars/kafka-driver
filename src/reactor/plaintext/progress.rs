//! Exact read and write progress returned without deciding connection policy.

use kafka_driver_core::{CallId, EffectId};

/// Why one bounded nonblocking read drive stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum ReadState {
    /// The socket currently reports that another read would block.
    Blocked,
    /// A signal interrupted the read and the owner should retry in a later step.
    Interrupted,
    /// A byte or frame fairness budget was reached and local progress remains possible.
    BudgetExhausted,
    /// The peer closed its write half after any reported progress.
    PeerClosed,
}

/// Progress made by one bounded read drive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct ReadProgress {
    bytes: usize,
    frames: usize,
    state: ReadState,
}

impl ReadProgress {
    pub(super) const fn new(bytes: usize, frames: usize, state: ReadState) -> Self {
        Self {
            bytes,
            frames,
            state,
        }
    }

    pub(in crate::reactor) const fn bytes(self) -> usize {
        self.bytes
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn frames(self) -> usize {
        self.frames
    }

    pub(in crate::reactor) const fn state(self) -> ReadState {
        self.state
    }
}

/// One complete encoded request removed from the ordered writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct CompletedWrite {
    call_id: CallId,
    effect_id: EffectId,
    frame_bytes: usize,
}

impl CompletedWrite {
    pub(super) const fn new(call_id: CallId, effect_id: EffectId, frame_bytes: usize) -> Self {
        Self {
            call_id,
            effect_id,
            frame_bytes,
        }
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn call_id(self) -> CallId {
        self.call_id
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn effect_id(self) -> EffectId {
        self.effect_id
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn frame_bytes(self) -> usize {
        self.frame_bytes
    }
}

/// Why one bounded nonblocking write drive stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum WriteState {
    /// No encoded frame remains queued.
    Idle,
    /// The socket currently reports that another write would block.
    Blocked,
    /// A signal interrupted the write and the owner should retry in a later step.
    Interrupted,
    /// The byte fairness budget was reached while queued frames remain.
    BudgetExhausted,
}

/// Progress made by one bounded write drive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct WriteDrive {
    bytes: usize,
    completed: usize,
    state: WriteState,
}

impl WriteDrive {
    pub(super) const fn new(bytes: usize, completed: usize, state: WriteState) -> Self {
        Self {
            bytes,
            completed,
            state,
        }
    }

    pub(in crate::reactor) const fn bytes(self) -> usize {
        self.bytes
    }

    pub(in crate::reactor) const fn completed(self) -> usize {
        self.completed
    }

    pub(in crate::reactor) const fn state(self) -> WriteState {
        self.state
    }
}
