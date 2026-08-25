//! Capacity-one direct broker state and sole-selector hosting surface.

use std::{io, time::Duration};

#[cfg(test)]
use bornera::TcpTransport;
use bornera::{ConnectionSet, ConnectionToken, RegisteredTransport};
use calandria::{Next, Span, Turn, WaitOutcome, WorkCount};
use kafka_driver_core::{CallFailure, Delivery, KafkaSessionMachine, Moment};
use kafka_wire::OutboundFrameLimits;
use kafka_wire_core::DecodeLimits;

use crate::{RequestError, config::ClientId, request::ErasedRequest};

use super::{operation_owner::DirectOperationContext, pending::PendingRequests};
use crate::reactor::bornera::{KafkaFrame, KafkaReplyClassifier, OperationContexts};

use super::decoder_gate::DirectFrameDecoder;

pub(super) type DirectSet<T> = ConnectionSet<DirectFrameDecoder, KafkaReplyClassifier, T>;

pub(super) const ID: u64 = 1;

pub(in crate::reactor) struct DirectOwner<T: RegisteredTransport> {
    pub(super) set: DirectSet<T>,
    pub(super) connection: ConnectionToken,
    pub(super) session: KafkaSessionMachine,
    pub(super) contexts: OperationContexts<DirectOperationContext>,
    pub(super) pending: PendingRequests,
    pub(super) client_id: Option<ClientId>,
    pub(super) outbound_limits: OutboundFrameLimits,
    pub(super) decode_limits: DecodeLimits,
    pub(super) negotiation_limits: crate::negotiation::NegotiationLimits,
    pub(super) negotiation_timeout: Duration,
    pub(super) response_capacity: usize,
    pub(super) write_frame_capacity: usize,
    pub(super) write_byte_capacity: usize,
    pub(super) write_frame_rejections: u64,
    pub(super) write_byte_rejections: u64,
    pub(super) last_close_reason: Option<kafka_driver_core::CloseReason>,
    pub(super) retired_seed: Option<crate::SeedSnapshot>,
    pub(super) submission_budget: std::num::NonZeroUsize,
    pub(super) last_turn: Turn,
    pub(super) admission_open: bool,
    pub(super) terminal: bool,
    pub(super) pending_recovery: Option<DirectRecovery>,
}

#[cfg(test)]
pub(in crate::reactor) type DirectPlaintextOwner = DirectOwner<TcpTransport>;

impl<T: RegisteredTransport> DirectOwner<T> {
    pub(in crate::reactor) fn submit(
        &mut self,
        request: Box<dyn ErasedRequest>,
        now: Moment,
        causality: &mut crate::reactor::causality::CausalSequence,
    ) -> io::Result<()> {
        self.submit_request(request, now, causality)
    }

    pub(in crate::reactor) fn wait(&mut self, maximum: Span) -> io::Result<WaitOutcome> {
        self.set.poll_io(maximum).map_err(message)
    }

    pub(in crate::reactor) fn wake_handle(&self) -> calandria::WakeHandle {
        self.set.wake_handle()
    }

    pub(in crate::reactor) fn pulse_handle(&self) -> bornera::ConnectionPulseHandle {
        self.set.pulse_handle()
    }

    pub(in crate::reactor) fn next_deadline(&self) -> Option<Moment> {
        if self.terminal {
            return None;
        }
        let engine = match self.last_turn.next() {
            Next::Now => Some(Moment::from_nanos(0)),
            Next::WakeOr(deadline) => Some(Moment::from_nanos(deadline.moment().as_nanos())),
            Next::Wake | Next::Stop => None,
        };
        engine.into_iter().chain(self.pending.next_deadline()).min()
    }

    pub(in crate::reactor) fn has_local_work(&self) -> bool {
        !self.terminal
            && (self.pending_recovery.is_some()
                || matches!(self.last_turn.next(), Next::Now)
                || (self.admission_open && !self.pending.is_empty()))
    }

    pub(in crate::reactor) const fn is_terminal(&self) -> bool {
        self.terminal
    }

    pub(super) fn mark_runnable(&mut self) {
        self.last_turn = Turn::runnable(WorkCount::new(1));
    }

    #[cfg(test)]
    pub(in crate::reactor) fn selector_registrations(&self) -> usize {
        self.set.snapshot().poller.registrations()
    }
}

pub(super) const fn calandria_moment(moment: Moment) -> calandria::Moment {
    calandria::Moment::from_nanos(moment.as_nanos())
}

pub(super) fn deadline_exceeded() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::DeadlineExceeded,
        delivery: Delivery::NotSent,
    }
}

pub(super) fn message(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

pub(super) fn add(now: Moment, duration: Duration) -> io::Result<Moment> {
    now.checked_add(duration)
        .ok_or_else(|| io::Error::other("direct broker deadline overflowed"))
}

pub(super) type DirectRecovery = bornera::RecoveryReport<bornera::OutboundFrame, KafkaFrame>;
