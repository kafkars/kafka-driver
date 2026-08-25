//! Small transport facade over one transport-generic direct semantic owner.

use std::io;

use bornera::TcpTransport;
use calandria::{Span, WaitOutcome};
use kafka_driver_core::Moment;

use crate::{
    SeedSnapshot,
    config::{DirectBrokerConfig, DriverLimits},
    request::ErasedRequest,
};

use super::owner::DirectOwner;
#[cfg(feature = "tls-rustls")]
use super::rustls_transport::DirectRustlsTransport;
use crate::reactor::causality::CausalSequence;

/// Exclusive direct owner for exactly one configured transport family.
pub(in crate::reactor) enum DirectBackend {
    Plaintext(Box<DirectOwner<TcpTransport>>),
    #[cfg(feature = "tls-rustls")]
    Rustls(Box<DirectOwner<DirectRustlsTransport>>),
}

impl DirectBackend {
    pub(in crate::reactor) fn new(
        limits: &DriverLimits,
        config: DirectBrokerConfig,
        now: Moment,
    ) -> io::Result<Self> {
        match config {
            DirectBrokerConfig::Plaintext { address, client_id } => Ok(Self::Plaintext(Box::new(
                DirectOwner::<TcpTransport>::new(limits, address, client_id, now)?,
            ))),
            #[cfg(feature = "tls-rustls")]
            DirectBrokerConfig::Rustls {
                address,
                tls,
                client_id,
            } => Ok(Self::Rustls(Box::new(
                DirectOwner::<DirectRustlsTransport>::new(limits, address, tls, client_id, now)?,
            ))),
        }
    }

    pub(in crate::reactor) fn submit(
        &mut self,
        request: Box<dyn ErasedRequest>,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<()> {
        match self {
            Self::Plaintext(owner) => owner.submit(request, now, causality),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(owner) => owner.submit(request, now, causality),
        }
    }

    pub(in crate::reactor) fn drive(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        match self {
            Self::Plaintext(owner) => owner.drive(now, causality),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(owner) => owner.drive(now, causality),
        }
    }

    pub(in crate::reactor) fn begin_session_drain(&mut self, now: Moment) -> io::Result<()> {
        match self {
            Self::Plaintext(owner) => owner.begin_session_drain(now),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(owner) => owner.begin_session_drain(now),
        }
    }

    pub(in crate::reactor) fn wait(&mut self, maximum: Span) -> io::Result<WaitOutcome> {
        match self {
            Self::Plaintext(owner) => owner.wait(maximum),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(owner) => owner.wait(maximum),
        }
    }

    pub(in crate::reactor) fn wake_handle(&self) -> calandria::WakeHandle {
        match self {
            Self::Plaintext(owner) => owner.wake_handle(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(owner) => owner.wake_handle(),
        }
    }

    pub(in crate::reactor) fn pulse_handle(&self) -> bornera::ConnectionPulseHandle {
        match self {
            Self::Plaintext(owner) => owner.pulse_handle(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(owner) => owner.pulse_handle(),
        }
    }

    pub(in crate::reactor) fn next_deadline(&self) -> Option<Moment> {
        match self {
            Self::Plaintext(owner) => owner.next_deadline(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(owner) => owner.next_deadline(),
        }
    }

    pub(in crate::reactor) fn has_local_work(&self) -> bool {
        match self {
            Self::Plaintext(owner) => owner.has_local_work(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(owner) => owner.has_local_work(),
        }
    }

    pub(in crate::reactor) fn is_terminal(&self) -> bool {
        match self {
            Self::Plaintext(owner) => owner.is_terminal(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(owner) => owner.is_terminal(),
        }
    }

    pub(in crate::reactor) fn seed_snapshot(&self) -> Option<SeedSnapshot> {
        match self {
            Self::Plaintext(owner) => owner.seed_snapshot(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(owner) => owner.seed_snapshot(),
        }
    }
}
