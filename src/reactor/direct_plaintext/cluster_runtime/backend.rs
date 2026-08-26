//! Transport-erased ownership of one typed cluster runtime and its plan factory.

#![allow(
    dead_code,
    reason = "activated with the pending Bornera cluster backend cutover"
)]

use std::io;

use bornera::TcpTransport;
use calandria::{Span, WaitOutcome};
use kafka_driver_core::Moment;

use crate::{
    DriverLimits,
    config::BrokerTemplate,
    reactor::{bootstrap::ResolvedSeed, broker::BrokerLimits, causality::CausalSequence},
};

#[cfg(feature = "tls-rustls")]
use crate::reactor::direct_plaintext::lane_plan::factory::RustlsLanePlanFactory;
use crate::reactor::direct_plaintext::lane_plan::factory::{
    BorneraEndpointFamily, PlaintextLanePlanFactory,
};
#[cfg(feature = "tls-rustls")]
use crate::reactor::direct_plaintext::rustls_transport::DirectRustlsTransport;

use super::{ClusterRuntime, seed::ResolvedSeedReplacement};

/// One transport family chosen before its sole Bornera selector is constructed.
pub(in crate::reactor::direct_plaintext) enum ClusterBackend {
    Plaintext {
        runtime: Box<ClusterRuntime<TcpTransport>>,
        factory: PlaintextLanePlanFactory,
    },
    #[cfg(feature = "tls-rustls")]
    Rustls {
        runtime: Box<ClusterRuntime<DirectRustlsTransport>>,
        factory: RustlsLanePlanFactory,
    },
}

impl ClusterBackend {
    pub(in crate::reactor::direct_plaintext) fn new(
        driver: &DriverLimits,
        template: BrokerTemplate,
    ) -> io::Result<Self> {
        let family =
            BorneraEndpointFamily::from_template(driver, BrokerLimits::default(), template);
        match family {
            BorneraEndpointFamily::Plaintext(factory) => Ok(Self::Plaintext {
                runtime: Box::new(ClusterRuntime::new(driver)?),
                factory,
            }),
            #[cfg(feature = "tls-rustls")]
            BorneraEndpointFamily::Rustls(factory) => Ok(Self::Rustls {
                runtime: Box::new(ClusterRuntime::new(driver)?),
                factory,
            }),
        }
    }

    pub(in crate::reactor::direct_plaintext) fn install_resolved_seed(
        &mut self,
        seed: ResolvedSeed,
        now: Moment,
    ) -> io::Result<()> {
        match self {
            Self::Plaintext { runtime, factory } => runtime
                .install_resolved_seed(factory, seed, now)
                .map(|_| ()),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, factory } => runtime
                .install_resolved_seed(factory, seed, now)
                .map(|_| ()),
        }
    }

    pub(in crate::reactor::direct_plaintext) fn replace_resolved_seed(
        &mut self,
        seed: ResolvedSeed,
        now: Moment,
    ) -> io::Result<ResolvedSeedReplacement> {
        match self {
            Self::Plaintext { runtime, factory } => {
                runtime.replace_resolved_seed(factory, seed, now)
            }
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, factory } => runtime.replace_resolved_seed(factory, seed, now),
        }
    }

    pub(in crate::reactor::direct_plaintext) fn drive(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        match self {
            Self::Plaintext { runtime, factory } => {
                runtime.drive_with_factory(factory, now, causality)
            }
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, factory } => {
                runtime.drive_with_factory(factory, now, causality)
            }
        }
    }

    pub(in crate::reactor::direct_plaintext) fn wait(
        &mut self,
        maximum: Span,
    ) -> io::Result<WaitOutcome> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.wait(maximum),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.wait(maximum),
        }
    }

    pub(in crate::reactor::direct_plaintext) fn wake_handle(&self) -> calandria::WakeHandle {
        match self {
            Self::Plaintext { runtime, .. } => runtime.connections.wake_handle(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.connections.wake_handle(),
        }
    }

    pub(in crate::reactor::direct_plaintext) fn pulse_handle(
        &self,
    ) -> bornera::ConnectionPulseHandle {
        match self {
            Self::Plaintext { runtime, .. } => runtime.connections.pulse_handle(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.connections.pulse_handle(),
        }
    }

    pub(in crate::reactor::direct_plaintext) fn next_deadline(&self) -> Option<Moment> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.next_deadline(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.next_deadline(),
        }
    }

    pub(in crate::reactor::direct_plaintext) fn has_local_work(&self) -> bool {
        match self {
            Self::Plaintext { runtime, .. } => runtime.has_local_work(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.has_local_work(),
        }
    }
}

#[cfg(test)]
#[path = "backend_test.rs"]
mod test;
