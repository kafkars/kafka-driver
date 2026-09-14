//! Plain TCP readiness barrier that lets accepted replies reach Kafka classification first.

use std::{
    io::{self, Read, Write},
    net::SocketAddr,
};

use bornera::{
    RegisteredTransport, SlotTransport, TcpSocketPolicy, TcpTransport, TransportBudget,
    TransportConnector, TransportError, TransportLimits, TransportPressure, TransportProgress,
};
use calandria::{Interest, Readiness};
use mio::{Registry, Token, event::Source};

use super::decoder_gate::DecoderGate;

/// Preserves reply-before-EOF ordering across Bornera's read and decode preferences.
pub(in crate::reactor) struct DirectPlaintextTransport {
    inner: TcpTransport,
    decoder_gate: DecoderGate,
}

impl Read for DirectPlaintextTransport {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buffer)
    }
}

impl Write for DirectPlaintextTransport {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.inner.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl SlotTransport for DirectPlaintextTransport {
    fn drive_establishment(
        &mut self,
        policy: TcpSocketPolicy,
        budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        self.inner.drive_establishment(policy, budget)
    }

    fn drive_transport(
        &mut self,
        budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        self.inner.drive_transport(budget)
    }

    fn begin_shutdown(
        &mut self,
        budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        self.inner.begin_shutdown(budget)
    }

    fn can_establish(&self) -> bool {
        self.inner.can_establish()
    }

    fn has_transport_work(&self) -> bool {
        self.inner.has_transport_work()
    }

    fn is_shutdown_complete(&self) -> bool {
        self.inner.is_shutdown_complete()
    }

    fn is_open(&self) -> bool {
        self.inner.is_open()
    }

    fn can_read(&self) -> bool {
        !self.decoder_gate.has_pending_decode() && self.inner.can_read()
    }

    fn can_write(&self) -> bool {
        self.inner.can_write()
    }

    fn desired_interest(&self, has_writes: bool) -> Interest {
        self.inner.desired_interest(has_writes)
    }

    fn pressure(&self) -> TransportPressure {
        self.inner.pressure()
    }

    fn pressure_limit(&self) -> TransportLimits {
        self.inner.pressure_limit()
    }

    fn clear_read(&mut self) {
        self.inner.clear_read();
    }

    fn clear_write(&mut self) {
        self.inner.clear_write();
    }
}

impl RegisteredTransport for DirectPlaintextTransport {
    fn observe_readiness(&mut self, readiness: Readiness) {
        self.inner.observe_readiness(readiness);
    }
}

impl Source for DirectPlaintextTransport {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        Source::register(&mut self.inner, registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        Source::reregister(&mut self.inner, registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        Source::deregister(&mut self.inner, registry)
    }
}

pub(super) struct DirectPlaintextConnector {
    decoder_gate: DecoderGate,
}

impl DirectPlaintextConnector {
    pub(super) const fn new(decoder_gate: DecoderGate) -> Self {
        Self { decoder_gate }
    }
}

impl TransportConnector for DirectPlaintextConnector {
    type Transport = DirectPlaintextTransport;

    fn connect(self, address: SocketAddr, _limits: TransportLimits) -> io::Result<Self::Transport> {
        TcpTransport::connect(address).map(|inner| DirectPlaintextTransport {
            inner,
            decoder_gate: self.decoder_gate,
        })
    }
}
