//! Test-only control surface over a Bornera-owned modeled transport.

use std::{
    io,
    net::SocketAddr,
    ops::{Deref, DerefMut},
};

use kafka_driver_core::Moment;

use crate::config::DriverLimits;

use super::{
    DirectBackend,
    attempt::{SimulatedTransport, SimulatedTransportHandle},
    runtime::DirectRuntime,
};

pub(in crate::reactor) struct SimulatedBackend {
    runtime: DirectRuntime<SimulatedTransport>,
    control: SimulatedTransportHandle,
}

impl SimulatedBackend {
    fn new(driver: &DriverLimits, address: SocketAddr, now: Moment) -> io::Result<Self> {
        let control = SimulatedTransportHandle::default();
        let runtime = DirectRuntime::new_simulated(driver, address, control.clone(), now)?;
        Ok(Self { runtime, control })
    }
}

impl Deref for SimulatedBackend {
    type Target = DirectRuntime<SimulatedTransport>;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl DerefMut for SimulatedBackend {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.runtime
    }
}

impl DirectBackend {
    pub(in crate::reactor) fn simulated(
        limits: &DriverLimits,
        address: SocketAddr,
        now: Moment,
    ) -> io::Result<Self> {
        Ok(Self::Simulated(Box::new(SimulatedBackend::new(
            limits, address, now,
        )?)))
    }

    pub(in crate::reactor) fn simulate_connect(&mut self) -> bool {
        match self {
            Self::Simulated(owner) => owner.control.connect(),
            Self::Plaintext(_) => false,
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(_) => false,
        }
    }

    pub(in crate::reactor) fn simulate_receive(&mut self, bytes: &[u8]) -> bool {
        match self {
            Self::Simulated(owner) => owner.control.receive(bytes),
            Self::Plaintext(_) => false,
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(_) => false,
        }
    }

    pub(in crate::reactor) fn take_simulated_frames(&mut self) -> Vec<Vec<u8>> {
        match self {
            Self::Simulated(owner) => owner.control.take_frames(),
            Self::Plaintext(_) => Vec::new(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(_) => Vec::new(),
        }
    }

    pub(in crate::reactor) fn simulated_connection_for_test(&self) -> bornera::ConnectionToken {
        match self {
            Self::Simulated(owner) => owner.lane.connection_for_test(),
            Self::Plaintext(_) => panic!("test backend is not simulated"),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(_) => panic!("test backend is not simulated"),
        }
    }
}
