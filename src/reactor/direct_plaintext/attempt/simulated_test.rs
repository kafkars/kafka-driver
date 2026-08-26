//! Test-only modeled transport connected through Bornera's production set.

use std::{
    cell::RefCell,
    io::{self, Read, Write},
    net::SocketAddr,
    rc::Rc,
};

use bornera::{
    ConnectionToken, RegisteredTransport, SlotTransport, TcpSocketPolicy, TransportBudget,
    TransportConnector, TransportError, TransportLimits, TransportPressure, TransportProgress,
};
use bornera_core::ConnectionEpoch;
use calandria::{Interest, Readiness, RetainedBytes};
use kafka_driver_core::Moment;
use mio::{Registry, Token, event::Source};

use super::{
    BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt, connection_config,
    plaintext_connect_error,
};
use crate::{
    config::DriverLimits,
    reactor::{
        bornera::KafkaReplyClassifier,
        broker::BrokerLimits,
        direct_plaintext::{limits::slot_limits, owner::DirectSet},
    },
};

const RETAINED_LIMIT: usize = 1024 * 1024;

#[derive(Clone, Debug, Default)]
pub(in crate::reactor::direct_plaintext) struct SimulatedTransportHandle(
    Rc<RefCell<SimulatedState>>,
);

#[derive(Debug, Default)]
struct SimulatedState {
    connected: bool,
    inbound: Vec<u8>,
    outbound: Vec<u8>,
}

impl SimulatedTransportHandle {
    pub(in crate::reactor::direct_plaintext) fn connect(&self) -> bool {
        let mut state = self.0.borrow_mut();
        let changed = !state.connected;
        state.connected = true;
        changed
    }

    pub(in crate::reactor::direct_plaintext) fn receive(&self, bytes: &[u8]) -> bool {
        let mut state = self.0.borrow_mut();
        let retained = state
            .inbound
            .len()
            .saturating_add(state.outbound.len())
            .saturating_add(bytes.len());
        if !state.connected || retained > RETAINED_LIMIT {
            return false;
        }
        state.inbound.extend_from_slice(bytes);
        true
    }

    pub(in crate::reactor::direct_plaintext) fn take_frames(&self) -> Vec<Vec<u8>> {
        let mut state = self.0.borrow_mut();
        let mut frames = Vec::new();
        loop {
            let Some(prefix) = state.outbound.get(..4) else {
                break;
            };
            let length = i32::from_be_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]);
            let Ok(length) = usize::try_from(length) else {
                break;
            };
            let Some(frame_length) = length.checked_add(4) else {
                break;
            };
            if state.outbound.len() < frame_length {
                break;
            }
            frames.push(state.outbound.drain(..frame_length).collect());
        }
        if state.outbound.is_empty() {
            state.outbound = Vec::new();
        }
        frames
    }

    fn pressure(&self) -> TransportPressure {
        let state = self.0.borrow();
        TransportPressure::new(
            retained(state.inbound.capacity()),
            retained(state.outbound.capacity()),
            RetainedBytes::ZERO,
            RetainedBytes::ZERO,
        )
        .unwrap_or(TransportPressure::MAX)
    }
}

fn retained(value: usize) -> RetainedBytes {
    RetainedBytes::new(u64::try_from(value).unwrap_or(u64::MAX))
}

pub(in crate::reactor::direct_plaintext) struct SimulatedAttempt {
    driver: DriverLimits,
    broker: BrokerLimits,
    handle: SimulatedTransportHandle,
}

impl SimulatedAttempt {
    pub(in crate::reactor::direct_plaintext) const fn new(
        driver: &DriverLimits,
        broker: BrokerLimits,
        handle: SimulatedTransportHandle,
    ) -> Self {
        Self {
            driver: *driver,
            broker,
            handle,
        }
    }
}

impl DirectConnectionAttempt<SimulatedTransport> for SimulatedAttempt {
    fn connect(
        &self,
        set: &mut DirectSet<SimulatedTransport>,
        owner: BorneraLaneOwner,
        address: SocketAddr,
        epoch: ConnectionEpoch,
        now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        let transport_limits = TransportLimits::new(retained(RETAINED_LIMIT));
        let (decoder, slot) = slot_limits(&self.driver, self.broker, transport_limits, None)
            .map_err(DirectConnectError::fatal)?;
        set.connect_with(
            connection_config(owner, address, epoch, now, self.broker)
                .map_err(DirectConnectError::fatal)?,
            slot,
            decoder,
            KafkaReplyClassifier,
            SimulatedConnector(self.handle.clone()),
        )
        .map_err(plaintext_connect_error)
    }
}

struct SimulatedConnector(SimulatedTransportHandle);

impl TransportConnector for SimulatedConnector {
    type Transport = SimulatedTransport;

    fn connect(self, _address: SocketAddr, limits: TransportLimits) -> io::Result<Self::Transport> {
        Ok(SimulatedTransport {
            handle: self.0,
            limits,
            open: false,
        })
    }
}

pub(in crate::reactor) struct SimulatedTransport {
    handle: SimulatedTransportHandle,
    limits: TransportLimits,
    open: bool,
}

impl Read for SimulatedTransport {
    fn read(&mut self, destination: &mut [u8]) -> io::Result<usize> {
        let mut state = self.handle.0.borrow_mut();
        if !self.open || state.inbound.is_empty() {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let copied = destination.len().min(state.inbound.len());
        destination[..copied].copy_from_slice(&state.inbound[..copied]);
        drop(state.inbound.drain(..copied));
        if state.inbound.is_empty() {
            state.inbound = Vec::new();
        }
        Ok(copied)
    }
}

impl Write for SimulatedTransport {
    fn write(&mut self, source: &[u8]) -> io::Result<usize> {
        let mut state = self.handle.0.borrow_mut();
        let retained = state
            .inbound
            .len()
            .saturating_add(state.outbound.len())
            .saturating_add(source.len());
        if !self.open || retained > RETAINED_LIMIT {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        state.outbound.extend_from_slice(source);
        Ok(source.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SlotTransport for SimulatedTransport {
    fn drive_establishment(
        &mut self,
        _policy: TcpSocketPolicy,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        self.open = true;
        Ok(TransportProgress::operation())
    }

    fn drive_transport(
        &mut self,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        Ok(TransportProgress::IDLE)
    }

    fn begin_shutdown(
        &mut self,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        Ok(TransportProgress::operation())
    }

    fn can_establish(&self) -> bool {
        !self.open && self.handle.0.borrow().connected
    }

    fn has_transport_work(&self) -> bool {
        false
    }

    fn is_shutdown_complete(&self) -> bool {
        true
    }

    fn is_open(&self) -> bool {
        self.open
    }

    fn can_read(&self) -> bool {
        self.open && !self.handle.0.borrow().inbound.is_empty()
    }

    fn can_write(&self) -> bool {
        self.open
    }

    fn desired_interest(&self, _has_writes: bool) -> Interest {
        Interest::READ_WRITE
    }

    fn pressure(&self) -> TransportPressure {
        self.handle.pressure()
    }

    fn pressure_limit(&self) -> TransportLimits {
        self.limits
    }

    fn clear_read(&mut self) {}

    fn clear_write(&mut self) {}
}

impl RegisteredTransport for SimulatedTransport {
    fn observe_readiness(&mut self, _readiness: Readiness) {}
}

impl Source for SimulatedTransport {
    fn register(
        &mut self,
        _registry: &Registry,
        _token: Token,
        _interests: mio::Interest,
    ) -> io::Result<()> {
        Ok(())
    }

    fn reregister(
        &mut self,
        _registry: &Registry,
        _token: Token,
        _interests: mio::Interest,
    ) -> io::Result<()> {
        Ok(())
    }

    fn deregister(&mut self, _registry: &Registry) -> io::Result<()> {
        Ok(())
    }
}
