//! Bornera-owned selector fixture for cross-thread wake scenarios.

use std::{io, time::Duration};

use calandria::{Span, WaitOutcome};
use kafka_driver_core::Moment;

use super::{WakeHandle, direct_plaintext::DirectBackend};

pub(in crate::reactor) struct WakeFixture {
    backend: DirectBackend,
}

impl WakeFixture {
    pub(in crate::reactor) fn new() -> io::Result<Self> {
        Ok(Self {
            backend: DirectBackend::simulated(
                &crate::DriverLimits::default(),
                std::net::SocketAddr::from(([127, 0, 0, 1], 9092)),
                Moment::ORIGIN,
            )?,
        })
    }

    pub(in crate::reactor) fn internal_wake(&self) -> calandria::WakeHandle {
        self.backend.wake_handle()
    }

    pub(in crate::reactor) fn public_wake(&self) -> WakeHandle {
        WakeHandle::bornera(self.backend.pulse_handle())
    }

    pub(in crate::reactor) fn wait(&mut self, maximum: Duration) -> io::Result<WaitOutcome> {
        let maximum = Span::try_from(maximum).unwrap_or(Span::from_nanos(u64::MAX));
        self.backend.wait(maximum)
    }
}
