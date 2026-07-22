//! Bounded FIFO ownership of calls awaiting write admission or broker response.

use std::collections::{VecDeque, vec_deque};

use crate::{CallId, Delivery, EffectId, Moment, TimerId};

use super::CorrelationId;

/// External progress reached by one pending call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingPhase {
    /// A write effect exists but has not been accepted by the transport writer.
    AwaitingWrite,
    /// The writer accepted the complete frame and a response may arrive.
    AwaitingResponse,
}

/// Immutable view of one call's connection-local response obligation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingCall {
    call_id: CallId,
    correlation_id: CorrelationId,
    write_effect: EffectId,
    deadline_timer: TimerId,
    deadline: Moment,
    delivery: Delivery,
    phase: PendingPhase,
}

impl PendingCall {
    pub(super) const fn new(
        call_id: CallId,
        correlation_id: CorrelationId,
        write_effect: EffectId,
        deadline_timer: TimerId,
        deadline: Moment,
    ) -> Self {
        Self {
            call_id,
            correlation_id,
            write_effect,
            deadline_timer,
            deadline,
            delivery: Delivery::NotSent,
            phase: PendingPhase::AwaitingWrite,
        }
    }

    /// Returns the public logical call identity.
    pub const fn call_id(self) -> CallId {
        self.call_id
    }

    /// Returns the connection-local Kafka correlation identity.
    pub const fn correlation_id(self) -> CorrelationId {
        self.correlation_id
    }

    /// Returns the write effect awaited by this call.
    pub const fn write_effect(self) -> EffectId {
        self.write_effect
    }

    /// Returns the deadline timer identity owned by this call.
    pub const fn deadline_timer(self) -> TimerId {
        self.deadline_timer
    }

    /// Returns the absolute driver-relative deadline.
    pub const fn deadline(self) -> Moment {
        self.deadline
    }

    /// Returns whether the broker may have received the request.
    pub const fn delivery(self) -> Delivery {
        self.delivery
    }

    /// Returns current write/response progress.
    pub const fn phase(self) -> PendingPhase {
        self.phase
    }

    pub(super) fn mark_submitted(&mut self) {
        self.phase = PendingPhase::AwaitingResponse;
        self.delivery = self.delivery.combine(Delivery::PossiblySent);
    }
}

#[derive(Debug)]
pub(super) struct PendingQueue {
    capacity: usize,
    calls: VecDeque<PendingCall>,
}

impl PendingQueue {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            calls: VecDeque::with_capacity(capacity),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.calls.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.calls.is_empty()
    }

    pub(super) fn is_full(&self) -> bool {
        self.calls.len() >= self.capacity
    }

    pub(super) fn push(&mut self, call: PendingCall) {
        self.calls.push_back(call);
    }

    pub(super) fn front(&self) -> Option<&PendingCall> {
        self.calls.front()
    }

    pub(super) fn pop_front(&mut self) -> Option<PendingCall> {
        self.calls.pop_front()
    }

    pub(super) fn by_write_effect_mut(&mut self, effect: EffectId) -> Option<&mut PendingCall> {
        self.calls
            .iter_mut()
            .find(|call| call.write_effect == effect)
    }

    pub(super) fn remove_awaiting_write(
        &mut self,
        call_id: CallId,
        effect_id: EffectId,
    ) -> Option<PendingCall> {
        let index = self.calls.iter().position(|pending| {
            pending.call_id == call_id
                && pending.write_effect == effect_id
                && pending.phase == PendingPhase::AwaitingWrite
        })?;
        self.calls.remove(index)
    }

    pub(super) fn by_timer(&self, timer: TimerId) -> Option<&PendingCall> {
        self.calls.iter().find(|call| call.deadline_timer == timer)
    }

    pub(super) fn contains_correlation(&self, correlation: CorrelationId) -> bool {
        self.calls
            .iter()
            .any(|call| call.correlation_id == correlation)
    }

    pub(super) fn has_identities(
        &self,
        call_id: CallId,
        write_effect: EffectId,
        deadline_timer: TimerId,
    ) -> IdentityConflicts {
        IdentityConflicts {
            call: self.calls.iter().any(|call| call.call_id == call_id),
            effect: self
                .calls
                .iter()
                .any(|call| call.write_effect == write_effect),
            timer: self
                .calls
                .iter()
                .any(|call| call.deadline_timer == deadline_timer),
        }
    }

    pub(super) fn iter(&self) -> vec_deque::Iter<'_, PendingCall> {
        self.calls.iter()
    }

    pub(super) fn drain(&mut self) -> vec_deque::Drain<'_, PendingCall> {
        self.calls.drain(..)
    }
}

pub(super) struct IdentityConflicts {
    pub(super) call: bool,
    pub(super) effect: bool,
    pub(super) timer: bool,
}
