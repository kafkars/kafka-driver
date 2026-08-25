//! Compatibility observation projected from Bornera and the Kafka session owner.

use bornera::{ConnectionReserveError, RegisteredTransport, TransportState};
use bornera_core::{ReserveError, RetainedBytes};
use kafka_driver_core::{
    BrokerCloseReason, BrokerState, CloseReason, ConnectionEpoch, ConnectionPhase,
    KafkaSessionCloseReason, KafkaSessionState, TransportFailure,
};

use crate::{SeedSnapshot, WriteQueueSnapshot};

use super::{failure_translation::connection_close_reason, owner::DirectOwner};

impl<T: RegisteredTransport> DirectOwner<T> {
    pub(in crate::reactor) fn seed_snapshot(&self) -> Option<SeedSnapshot> {
        self.live_seed_snapshot().or(self.retired_seed)
    }

    fn live_seed_snapshot(&self) -> Option<SeedSnapshot> {
        let mechanical = self.set.connection_snapshot(self.connection).ok()?;
        let epoch = ConnectionEpoch::from_raw(self.connection.epoch().get());
        let session = self.session.state();
        let broker_state = self.broker_state(session, epoch, mechanical.transport)?;
        let connection_phase = connection_phase(session, mechanical.transport, self.admission_open);
        let retained_bytes =
            usize::try_from(mechanical.buffered_write_retained_bytes.get()).unwrap_or(usize::MAX);
        Some(SeedSnapshot::new(
            broker_state,
            connection_phase,
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

    fn broker_state(
        &self,
        session: KafkaSessionState,
        epoch: ConnectionEpoch,
        transport: TransportState,
    ) -> Option<BrokerState> {
        match transport {
            TransportState::Closing => {
                return match session {
                    KafkaSessionState::Closing {
                        reason: KafkaSessionCloseReason::AuthenticationFailed(failure),
                    }
                    | KafkaSessionState::Closed {
                        reason: KafkaSessionCloseReason::AuthenticationFailed(failure),
                    } => Some(BrokerState::Closed {
                        reason: BrokerCloseReason::AuthenticationFailed(failure),
                    }),
                    KafkaSessionState::Draining { .. }
                    | KafkaSessionState::Closing {
                        reason:
                            KafkaSessionCloseReason::Requested | KafkaSessionCloseReason::Drained,
                    }
                    | KafkaSessionState::Closed {
                        reason:
                            KafkaSessionCloseReason::Requested | KafkaSessionCloseReason::Drained,
                    } => Some(BrokerState::Draining { epoch }),
                    _ => None,
                };
            }
            TransportState::Closed => {
                return match session {
                    KafkaSessionState::Closing {
                        reason: KafkaSessionCloseReason::AuthenticationFailed(failure),
                    }
                    | KafkaSessionState::Closed {
                        reason: KafkaSessionCloseReason::AuthenticationFailed(failure),
                    } => Some(BrokerState::Closed {
                        reason: BrokerCloseReason::AuthenticationFailed(failure),
                    }),
                    KafkaSessionState::Closed {
                        reason:
                            KafkaSessionCloseReason::Requested | KafkaSessionCloseReason::Drained,
                    } => Some(BrokerState::Closed {
                        reason: BrokerCloseReason::Requested,
                    }),
                    KafkaSessionState::Draining { .. }
                    | KafkaSessionState::Closing {
                        reason:
                            KafkaSessionCloseReason::Requested | KafkaSessionCloseReason::Drained,
                    } => Some(BrokerState::Draining { epoch }),
                    _ => None,
                };
            }
            TransportState::Connecting | TransportState::Open => {}
            _ => return None,
        }
        match session {
            KafkaSessionState::AwaitingTransport
            | KafkaSessionState::Negotiating { .. }
            | KafkaSessionState::Authenticating { .. } => {
                Some(BrokerState::Connecting { epoch, retry: None })
            }
            KafkaSessionState::Ready { .. }
                if self.admission_open && transport == TransportState::Open =>
            {
                Some(BrokerState::Available { epoch })
            }
            KafkaSessionState::Ready { .. } => Some(BrokerState::Connecting { epoch, retry: None }),
            KafkaSessionState::Draining { .. }
            | KafkaSessionState::Closing {
                reason: KafkaSessionCloseReason::Requested | KafkaSessionCloseReason::Drained,
            } => Some(BrokerState::Draining { epoch }),
            KafkaSessionState::Closed {
                reason: KafkaSessionCloseReason::Requested | KafkaSessionCloseReason::Drained,
            } => Some(BrokerState::Closed {
                reason: BrokerCloseReason::Requested,
            }),
            KafkaSessionState::Closing {
                reason: KafkaSessionCloseReason::AuthenticationFailed(failure),
            }
            | KafkaSessionState::Closed {
                reason: KafkaSessionCloseReason::AuthenticationFailed(failure),
            } => Some(BrokerState::Closed {
                reason: BrokerCloseReason::AuthenticationFailed(failure),
            }),
            // A fixed direct slot has no reconnect state capable of honestly
            // representing a generic fatal terminal failure.
            KafkaSessionState::Closing { .. } | KafkaSessionState::Closed { .. } => None,
        }
    }

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
        let Ok(snapshot) = self.set.connection_snapshot(self.connection) else {
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

    pub(super) fn record_session_close(&mut self, reason: KafkaSessionCloseReason) {
        self.last_close_reason = Some(session_close_reason(reason));
    }

    pub(super) fn latch_retired_seed(&mut self) {
        self.retired_seed = self.live_seed_snapshot();
    }

    pub(super) fn latch_recovered_seed(&mut self, recovered_epoch: u64) {
        let session = self.session.state();
        let epoch = ConnectionEpoch::from_raw(recovered_epoch);
        let Some(broker_state) = self.broker_state(session, epoch, TransportState::Closed) else {
            return;
        };
        self.retired_seed = Some(SeedSnapshot::new(
            broker_state,
            ConnectionPhase::Closed,
            self.last_close_reason,
            WriteQueueSnapshot::new(
                0,
                0,
                self.write_frame_rejections,
                self.write_byte_rejections,
            ),
        ));
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
