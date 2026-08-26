//! Connection-local Kafka ownership independent of the shared Bornera set.

use std::{
    io,
    ops::{Deref, DerefMut},
    time::Duration,
};

use bornera::{ConnectionSet, ConnectionToken, RegisteredTransport};
use kafka_driver_core::{CallFailure, Delivery, KafkaSessionMachine, Moment};
use kafka_wire::OutboundFrameLimits;
use kafka_wire_core::DecodeLimits;

use crate::{
    RequestError,
    authentication::AuthenticationSession,
    config::ClientId,
    reactor::scram_proof::{ScramProofFence, ScramProofSender},
};

use super::{operation_owner::DirectOperationContext, pending::PendingRequests};
use crate::reactor::bornera::{KafkaFrame, KafkaReplyClassifier, OperationContexts};

use super::{
    attempt::{BorneraLaneOwner, DirectConnectionAttempt},
    decoder_gate::DirectFrameDecoder,
    lane_plan::KafkaSessionPlan,
    lifecycle::DirectLifecycle,
};

use super::endpoint_refresh::DirectEndpointRefresh;

pub(super) type DirectSet<T> = ConnectionSet<DirectFrameDecoder, KafkaReplyClassifier, T>;

pub(super) const INITIAL_EPOCH: u64 = 1;
pub(super) const SET_OWNER_ID: u64 = 1;

pub(in crate::reactor) struct DirectLane<T: RegisteredTransport> {
    #[allow(dead_code, reason = "replayed by the direct reconnect lifecycle")]
    pub(super) connection_attempt: Box<dyn DirectConnectionAttempt<T>>,
    pub(super) connection_owner: BorneraLaneOwner,
    pub(super) connection: Option<ConnectionToken>,
    pub(super) addresses: crate::reactor::address_rotation::AddressRotation,
    pub(super) endpoint_refresh: Option<DirectEndpointRefresh>,
    pub(super) lifecycle: DirectLifecycle,
    #[allow(dead_code, reason = "replayed by the direct reconnect lifecycle")]
    pub(super) session_plan: KafkaSessionPlan,
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
    pub(super) runnable: bool,
    pub(super) admission_open: bool,
    pub(super) terminal: bool,
    pub(super) pending_recovery: Option<DirectRecovery>,
}

/// Temporary affine access to one lane through its shared mechanical owner.
pub(super) struct DirectLaneAccess<'a, T: RegisteredTransport> {
    pub(super) lane: &'a mut DirectLane<T>,
    pub(super) set: &'a mut DirectSet<T>,
}

pub(super) struct DirectLaneView<'a, T: RegisteredTransport> {
    pub(super) lane: &'a DirectLane<T>,
    pub(super) set: &'a DirectSet<T>,
}

impl<T: RegisteredTransport> Deref for DirectLaneAccess<'_, T> {
    type Target = DirectLane<T>;

    fn deref(&self) -> &Self::Target {
        self.lane
    }
}

impl<T: RegisteredTransport> DerefMut for DirectLaneAccess<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.lane
    }
}

impl<T: RegisteredTransport> Deref for DirectLaneView<'_, T> {
    type Target = DirectLane<T>;

    fn deref(&self) -> &Self::Target {
        self.lane
    }
}

impl<T: RegisteredTransport> DirectLane<T> {
    pub(in crate::reactor) fn is_terminal(&self) -> bool {
        self.terminal || self.lifecycle.is_closed()
    }

    pub(super) fn live_connection(&self) -> io::Result<ConnectionToken> {
        self.connection.ok_or_else(|| {
            io::Error::other("direct lifecycle has no live Bornera connection generation")
        })
    }

    pub(super) fn mark_runnable(&mut self) {
        self.runnable = true;
    }

    pub(super) fn mark_waiting(&mut self) {
        self.runnable = false;
    }
}

impl<T: RegisteredTransport> DirectLaneAccess<'_, T> {
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
        self.mark_waiting();
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
}

impl<T: RegisteredTransport> DirectLane<T> {
    #[cfg(test)]
    pub(in crate::reactor) fn connection_for_test(&self) -> ConnectionToken {
        self.connection
            .unwrap_or_else(|| panic!("test requires a live direct connection"))
    }
}

#[cfg(test)]
pub(in crate::reactor) type DirectPlaintextOwner =
    super::runtime::DirectRuntime<bornera::TcpTransport>;

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
