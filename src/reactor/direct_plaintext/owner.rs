//! Capacity-one direct plaintext broker state and sole-selector hosting surface.

use std::{io, net::SocketAddr, time::Duration};

use bornera::{
    ConnectionConfig, ConnectionIdentity, ConnectionSet, ConnectionSetConfig, ConnectionToken,
    RegisteredTransport, TcpTransport,
};
use bornera_core::{ConnectionEpoch, ConnectionId, EndpointId, LaneId};
use calandria::{
    Deadline, Next, ResourceOwnerId, Span, TimerOwnerId, Turn, WaitOutcome, WorkCount,
};
use kafka_driver_core::{CallFailure, Delivery, KafkaSessionLimits, KafkaSessionMachine, Moment};
use kafka_wire::OutboundFrameLimits;
use kafka_wire_core::DecodeLimits;

use crate::{
    RequestError,
    config::{ClientId, DriverLimits},
    request::ErasedRequest,
};

use super::{
    limits::{set_limits, slot_limits},
    operation_owner::DirectOperationContext,
    pending::PendingRequests,
};
use crate::reactor::{
    bornera::{KafkaFrame, KafkaFrameDecoder, KafkaReplyClassifier, OperationContexts},
    broker::BrokerLimits,
};

pub(super) type DirectSet<T> = ConnectionSet<KafkaFrameDecoder, KafkaReplyClassifier, T>;

const ID: u64 = 1;

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

pub(in crate::reactor) type DirectPlaintextOwner = DirectOwner<TcpTransport>;

impl DirectOwner<TcpTransport> {
    pub(in crate::reactor) fn new(
        driver: &DriverLimits,
        address: SocketAddr,
        client_id: Option<ClientId>,
        now: Moment,
    ) -> io::Result<Self> {
        let broker = BrokerLimits::default();
        let (decoder, slot) = slot_limits(driver, broker)?;
        let mut set = DirectSet::<TcpTransport>::new(
            ConnectionSetConfig::new(ResourceOwnerId::new(ID)),
            set_limits(driver),
        )
        .map_err(message)?;
        let connect_deadline = now
            .checked_add(broker.connect_timeout())
            .ok_or_else(|| io::Error::other("direct connect deadline overflowed"))?;
        let lane =
            u32::try_from(ID).map_err(|_| io::Error::other("direct lane identity exceeds u32"))?;
        let identity = ConnectionIdentity::new(
            EndpointId::new(ID),
            LaneId::new(lane),
            ConnectionId::new(ID),
            ConnectionEpoch::new(ID),
        );
        let connection = set
            .connect(
                ConnectionConfig::new(
                    identity,
                    address,
                    Deadline::at(calandria_moment(connect_deadline)),
                    TimerOwnerId::new(ID),
                ),
                slot,
                decoder,
                KafkaReplyClassifier,
            )
            .map_err(message)?;
        let retained = calandria::RetainedBytes::try_from(driver.mailbox_byte_capacity().get())
            .map_err(message)?;
        Ok(Self {
            set,
            connection,
            session: KafkaSessionMachine::new(KafkaSessionLimits::default()),
            contexts: OperationContexts::new(broker.response_capacity(), retained),
            pending: PendingRequests::new(
                driver.mailbox_capacity(),
                driver.mailbox_byte_capacity(),
            ),
            client_id,
            outbound_limits: broker.outbound_frame(),
            decode_limits: DecodeLimits::default(),
            negotiation_limits: broker.negotiation(),
            negotiation_timeout: broker.negotiation_timeout(),
            response_capacity: broker.response_capacity().get(),
            write_frame_capacity: broker.transport().write().max_queued_frames(),
            write_byte_capacity: broker.transport().write().max_buffered_bytes(),
            write_frame_rejections: 0,
            write_byte_rejections: 0,
            last_close_reason: None,
            retired_seed: None,
            submission_budget: driver.command_budget(),
            last_turn: Turn::waiting(),
            admission_open: false,
            terminal: false,
            pending_recovery: None,
        })
    }
}

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
        .ok_or_else(|| io::Error::other("direct plaintext deadline overflowed"))
}

pub(super) type DirectRecovery = bornera::RecoveryReport<bornera::OutboundFrame, KafkaFrame>;
