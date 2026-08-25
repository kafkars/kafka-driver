//! Rustls readiness barrier that lets accepted plaintext reach Kafka classification first.

use std::{
    io::{self, Read, Write},
    net::SocketAddr,
};

use bornera::{
    RegisteredTransport, SlotTransport, TcpSocketPolicy, TransportBudget, TransportConnector,
    TransportDiagnostic, TransportError, TransportFailureKind, TransportLimits, TransportPressure,
    TransportProgress,
};
use bornera_rustls::{RustlsConnector, RustlsTransport};
use calandria::{Interest, Readiness};
use mio::{Registry, Token, event::Source};

use super::decoder_gate::DecoderGate;

/// Preserves reply-before-EOF ordering across Bornera's transport and decode preferences.
pub(in crate::reactor) struct DirectRustlsTransport {
    inner: RustlsTransport,
    decoder_gate: DecoderGate,
    pending_error: Option<TransportDiagnostic>,
}

impl Read for DirectRustlsTransport {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buffer)
    }
}

impl Write for DirectRustlsTransport {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.inner.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl SlotTransport for DirectRustlsTransport {
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
        if let Some(diagnostic) = self.pending_error {
            if self.decoder_gate.has_pending_decode() {
                return Ok(TransportProgress::operation());
            }
            self.pending_error = None;
            return Err(TransportError::new(diagnostic));
        }
        match self.inner.drive_transport(budget) {
            Err(error)
                if self.decoder_gate.has_pending_decode()
                    && is_deferred_terminal(error.diagnostic()) =>
            {
                self.pending_error = Some(error.diagnostic());
                // A bounded synthetic operation rotates Bornera back to the
                // decoder until all already-fed plaintext has been offered.
                Ok(TransportProgress::operation())
            }
            result => result,
        }
    }

    fn begin_shutdown(
        &mut self,
        budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        if let Some(diagnostic) = self.pending_error.take() {
            return Err(TransportError::new(diagnostic));
        }
        self.inner.begin_shutdown(budget)
    }

    fn can_establish(&self) -> bool {
        self.inner.can_establish()
    }

    fn has_transport_work(&self) -> bool {
        self.pending_error.is_some() || self.inner.has_transport_work()
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

impl RegisteredTransport for DirectRustlsTransport {
    fn observe_readiness(&mut self, readiness: Readiness) {
        self.inner.observe_readiness(readiness);
    }
}

impl Source for DirectRustlsTransport {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.inner.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.inner.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        self.inner.deregister(registry)
    }
}

pub(super) struct DirectRustlsConnector {
    inner: RustlsConnector,
    decoder_gate: DecoderGate,
}

impl DirectRustlsConnector {
    pub(super) const fn new(inner: RustlsConnector, decoder_gate: DecoderGate) -> Self {
        Self {
            inner,
            decoder_gate,
        }
    }
}

impl TransportConnector for DirectRustlsConnector {
    type Transport = DirectRustlsTransport;

    fn connect(self, address: SocketAddr, limits: TransportLimits) -> io::Result<Self::Transport> {
        self.inner
            .connect(address, limits)
            .map(|inner| DirectRustlsTransport {
                inner,
                decoder_gate: self.decoder_gate,
                pending_error: None,
            })
    }
}

fn is_deferred_terminal(diagnostic: TransportDiagnostic) -> bool {
    diagnostic.failure == TransportFailureKind::Truncated
        || (diagnostic.failure == TransportFailureKind::Io
            && matches!(
                diagnostic.kind,
                io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::NotConnected
                    | io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::WriteZero
            ))
}
