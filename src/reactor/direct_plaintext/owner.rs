//! Capacity-one direct broker state and sole-selector hosting surface.

use std::{io, time::Duration};

#[cfg(test)]
use bornera::TcpTransport;
use bornera::{ConnectionSet, ConnectionToken, RegisteredTransport};
use calandria::{Next, Span, Turn, WaitOutcome, WorkCount};
use kafka_driver_core::{CallFailure, Delivery, KafkaSessionMachine, Moment};
use kafka_wire::OutboundFrameLimits;
use kafka_wire_core::DecodeLimits;

use crate::{
    RequestError,
    authentication::AuthenticationSession,
    config::ClientId,
    reactor::scram_proof::{ScramProofFence, ScramProofSender},
    request::ErasedRequest,
};

use super::{operation_owner::DirectOperationContext, pending::PendingRequests};
use crate::reactor::bornera::{KafkaFrame, KafkaReplyClassifier, OperationContexts};

use super::{
    attempt::DirectConnectionAttempt, decoder_gate::DirectFrameDecoder, lifecycle::DirectLifecycle,
    session_plan::DirectSessionPlan,
};

pub(super) type DirectSet<T> = ConnectionSet<DirectFrameDecoder, KafkaReplyClassifier, T>;

pub(super) const ID: u64 = 1;

pub(in crate::reactor) struct DirectOwner<T: RegisteredTransport> {
    pub(super) set: DirectSet<T>,
    #[allow(dead_code, reason = "replayed by the direct reconnect lifecycle")]
    pub(super) connection_attempt: Box<dyn DirectConnectionAttempt<T>>,
    pub(super) connection: Option<ConnectionToken>,
    pub(super) lifecycle: DirectLifecycle,
    #[allow(dead_code, reason = "replayed by the direct reconnect lifecycle")]
    pub(super) session_plan: DirectSessionPlan,
    pub(super) session: KafkaSessionMachine,
    pub(super) authentication_session: Option<AuthenticationSession>,
    pub(super) scram_proof_sender: Option<ScramProofSender>,
    pub(super) pending_scram_proof: Option<ScramProofFence>,
    pub(super) session_deadline: Option<Moment>,
    pub(super) contexts: OperationContexts<DirectOperationContext>,
    pub(super) pending: PendingRequests,
    pub(super) client_id: Option<ClientId>,
    pub(super) outbound_limits: OutboundFrameLimits,
    pub(super) decode_limits: DecodeLimits,
    pub(super) negotiation_limits: crate::negotiation::NegotiationLimits,
    pub(super) negotiation_timeout: Duration,
    pub(super) authentication_timeout: Duration,
    pub(super) response_capacity: usize,
    pub(super) write_frame_capacity: usize,
    pub(super) write_byte_capacity: usize,
    pub(super) write_frame_rejections: u64,
    pub(super) write_byte_rejections: u64,
    pub(super) generation_close_reason: Option<kafka_driver_core::CloseReason>,
    pub(super) last_close_reason: Option<kafka_driver_core::CloseReason>,
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
        match self.set.poll_io(maximum) {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                let primary = message(error);
                let Some(connection) = self.connection else {
                    let _ = self.generation_invariant_fatal(
                        Moment::ORIGIN,
                        None,
                        "Bornera readiness failed without a live direct generation",
                    );
                    return Err(primary);
                };
                match self.recover_failed_generation(connection, Moment::ORIGIN, None) {
                    Ok(report) => {
                        self.capture_recovery(report);
                        Ok(WaitOutcome::Notified)
                    }
                    Err(_) => Err(primary),
                }
            }
        }
    }

    pub(in crate::reactor) fn wake_handle(&self) -> calandria::WakeHandle {
        self.set.wake_handle()
    }

    pub(in crate::reactor) fn pulse_handle(&self) -> bornera::ConnectionPulseHandle {
        self.set.pulse_handle()
    }

    pub(in crate::reactor) fn next_deadline(&self) -> Option<Moment> {
        if self.is_terminal() {
            return None;
        }
        let engine = self
            .lifecycle
            .has_live_generation()
            .then(|| match self.last_turn.next() {
                Next::Now => Some(Moment::from_nanos(0)),
                Next::WakeOr(deadline) => Some(Moment::from_nanos(deadline.moment().as_nanos())),
                Next::Wake | Next::Stop => None,
            })
            .flatten();
        engine
            .into_iter()
            .chain(self.lifecycle.next_deadline())
            .chain(
                self.lifecycle
                    .has_live_generation()
                    .then_some(self.session_deadline)
                    .flatten(),
            )
            .chain(self.pending.next_deadline())
            .min()
    }

    pub(in crate::reactor) fn has_local_work(&self) -> bool {
        !self.is_terminal()
            && (self.pending_recovery.is_some()
                || (self.lifecycle.has_live_generation()
                    && matches!(self.last_turn.next(), Next::Now))
                || (self.admission_open && !self.pending.is_empty()))
    }

    pub(in crate::reactor) fn is_terminal(&self) -> bool {
        self.terminal || self.lifecycle.is_closed()
    }

    pub(super) fn mark_runnable(&mut self) {
        self.last_turn = Turn::runnable(WorkCount::new(1));
    }

    pub(super) fn live_connection(&self) -> io::Result<ConnectionToken> {
        self.connection.ok_or_else(|| {
            io::Error::other("direct lifecycle has no live Bornera connection generation")
        })
    }

    pub(super) fn capture_recovery(&mut self, report: DirectRecoveryReport) {
        self.capture_recovery_with(report, false);
    }

    pub(super) fn capture_diverged_recovery(&mut self, report: DirectRecoveryReport) {
        self.capture_recovery_with(report, true);
    }

    fn capture_recovery_with(&mut self, report: DirectRecoveryReport, semantic_diverged: bool) {
        let token_diverged = self
            .connection
            .take()
            .is_none_or(|connection| connection.epoch() != report.epoch);
        self.admission_open = false;
        self.session_deadline = None;
        self.clear_authentication_ownership();
        self.last_turn = Turn::waiting();
        if let Some(pending) = self.pending_recovery.as_mut() {
            pending.semantic_diverged = true;
            self.totalize_duplicate_recovery(report);
            return;
        }
        self.pending_recovery = Some(DirectRecovery {
            report,
            semantic_diverged: semantic_diverged || token_diverged,
        });
    }

    #[cfg(test)]
    pub(in crate::reactor) fn selector_registrations(&self) -> usize {
        self.set.snapshot().poller.registrations()
    }

    #[cfg(test)]
    pub(in crate::reactor) fn connection_for_test(&self) -> ConnectionToken {
        self.connection
            .unwrap_or_else(|| panic!("test requires a live direct connection"))
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

pub(super) type DirectRecoveryReport = bornera::RecoveryReport<bornera::OutboundFrame, KafkaFrame>;

pub(super) struct DirectRecovery {
    pub(super) report: DirectRecoveryReport,
    pub(super) semantic_diverged: bool,
}
