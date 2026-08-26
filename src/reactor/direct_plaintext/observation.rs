//! Public observation projected from long-lived policy and Bornera mechanics.

use bornera::{ConnectionReserveError, RegisteredTransport, TransportState};
use bornera_core::{ReserveError, RetainedBytes};
use kafka_driver_core::{
    CloseReason, ConnectionPhase, KafkaSessionCloseReason, KafkaSessionState, TransportFailure,
};

use crate::{SeedSnapshot, WriteQueueSnapshot};

use super::{
    failure_translation::connection_close_reason,
    owner::{DirectLaneAccess, DirectLaneView},
};

impl<T: RegisteredTransport> DirectLaneView<'_, T> {
    pub(in crate::reactor) fn seed_snapshot(&self) -> Option<SeedSnapshot> {
        if !self.terminal && self.lifecycle.has_live_generation() {
            return self.live_seed_snapshot();
        }
        if self.terminal && !self.lifecycle.is_closed() {
            return None;
        }
        Some(SeedSnapshot::new(
            self.lifecycle.state(),
            ConnectionPhase::Closed,
            self.last_close_reason,
            self.empty_write_queue(),
        ))
    }

    fn live_seed_snapshot(&self) -> Option<SeedSnapshot> {
        let mechanical = self.set.connection_snapshot(self.connection?).ok()?;
        let retained_bytes =
            usize::try_from(mechanical.buffered_write_retained_bytes.get()).unwrap_or(usize::MAX);
        Some(SeedSnapshot::new(
            self.lifecycle.state(),
            connection_phase(
                self.session.state(),
                mechanical.transport,
                self.admission_open,
            ),
            self.last_close_reason.or_else(|| {
                mechanical
                    .connection
                    .close_reason
                    .map(|reason| connection_close_reason(reason, mechanical.transport_diagnostic))
            }),
            WriteQueueSnapshot::new(
                mechanical.queued_write_frames,
                retained_bytes,
                self.write_frame_rejections,
                self.write_byte_rejections,
            ),
        ))
    }

    fn empty_write_queue(&self) -> WriteQueueSnapshot {
        WriteQueueSnapshot::new(
            0,
            0,
            self.write_frame_rejections,
            self.write_byte_rejections,
        )
    }
}

impl<T: RegisteredTransport> DirectLaneAccess<'_, T> {
    pub(super) fn observe_reserve_rejection(
        &mut self,
        error: ConnectionReserveError,
        incoming: RetainedBytes,
    ) {
        if !matches!(
            error,
            ConnectionReserveError::Rejected(ReserveError::WriteCapacity)
        ) {
            return;
        }
        let Some(connection) = self.connection else {
            return;
        };
        let Ok(snapshot) = self.set.connection_snapshot(connection) else {
            return;
        };
        if snapshot.connection.buffered_write_frames >= self.write_frame_capacity {
            self.write_frame_rejections = self.write_frame_rejections.saturating_add(1);
            return;
        }
        let retained = usize::try_from(snapshot.connection.buffered_write_retained_bytes.get())
            .unwrap_or(usize::MAX);
        let incoming = usize::try_from(incoming.get()).unwrap_or(usize::MAX);
        if retained
            .checked_add(incoming)
            .is_none_or(|accepted| accepted > self.write_byte_capacity)
        {
            self.write_byte_rejections = self.write_byte_rejections.saturating_add(1);
        }
    }

    pub(super) fn record_generation_close(&mut self, reason: KafkaSessionCloseReason) {
        self.generation_close_reason = Some(session_close_reason(reason));
    }
}

fn connection_phase(
    session: KafkaSessionState,
    transport: TransportState,
    admission_open: bool,
) -> ConnectionPhase {
    match transport {
        TransportState::Closing => return ConnectionPhase::Closing,
        TransportState::Closed => return ConnectionPhase::Closed,
        TransportState::Connecting | TransportState::Open | _ => {}
    }
    match session {
        KafkaSessionState::AwaitingTransport => ConnectionPhase::Opening,
        KafkaSessionState::Authenticating { .. } => ConnectionPhase::Authenticating,
        KafkaSessionState::Ready { .. } if admission_open => ConnectionPhase::Ready,
        KafkaSessionState::Negotiating { .. } | KafkaSessionState::Ready { .. } => {
            ConnectionPhase::Negotiating
        }
        KafkaSessionState::Draining { .. } => ConnectionPhase::Draining,
        KafkaSessionState::Closing { .. } => ConnectionPhase::Closing,
        KafkaSessionState::Closed { .. } => ConnectionPhase::Closed,
    }
}

const fn session_close_reason(reason: KafkaSessionCloseReason) -> CloseReason {
    match reason {
        KafkaSessionCloseReason::Drained => CloseReason::Drained,
        KafkaSessionCloseReason::Requested => CloseReason::Requested,
        KafkaSessionCloseReason::NegotiationFailed(failure) => {
            CloseReason::NegotiationFailed(failure)
        }
        KafkaSessionCloseReason::AuthenticationFailed(failure) => {
            CloseReason::AuthenticationFailed(failure)
        }
        KafkaSessionCloseReason::ProtocolFailed(_) => CloseReason::MalformedResponse,
        KafkaSessionCloseReason::TransportClosed => {
            CloseReason::TransportLost(TransportFailure::Other)
        }
    }
}
