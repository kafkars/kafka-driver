//! Persistent set construction with the first replayable connection attempt.

use std::{io, net::SocketAddr};

use bornera::{RegisteredTransport, TcpTransport};
use kafka_driver_core::Moment;

use crate::config::{BrokerAddresses, ClientId, DriverLimits, SaslConfig};
use crate::reactor::broker::BrokerLimits;

#[cfg(feature = "tls-rustls")]
use super::rustls_transport::DirectRustlsTransport;
#[cfg(test)]
use super::{
    attempt::{
        DirectConnectionAttempt, SimulatedAttempt, SimulatedTransport, SimulatedTransportHandle,
    },
    lane_plan::KafkaSessionPlan,
};
use super::{
    lane_construction::start_lane, lane_plan::BorneraLanePlan, limits::DirectSetBounds,
    runtime::DirectRuntime, set_owner::DirectSetOwner,
};
use crate::reactor::bornera::BorneraIdentityAllocator;

impl DirectRuntime<TcpTransport> {
    pub(in crate::reactor) fn new(
        driver: &DriverLimits,
        address: SocketAddr,
        sasl: Option<SaslConfig>,
        client_id: Option<ClientId>,
        now: Moment,
    ) -> io::Result<Self> {
        let broker = BrokerLimits::default();
        let plan = BorneraLanePlan::plaintext(
            driver,
            broker,
            BrokerAddresses::Direct(address),
            sasl,
            client_id,
        );
        start(driver, plan, now)
    }

    #[cfg(test)]
    pub(super) fn new_with_attempt(
        driver: &DriverLimits,
        address: SocketAddr,
        sasl: Option<SaslConfig>,
        attempt: Box<dyn DirectConnectionAttempt<TcpTransport>>,
        now: Moment,
    ) -> io::Result<Self> {
        let broker = BrokerLimits::default();
        let plan = BorneraLanePlan::new(
            BrokerAddresses::Direct(address),
            broker,
            None,
            KafkaSessionPlan::new(sasl, broker),
            attempt,
        );
        start(driver, plan, now)
    }
}

#[cfg(test)]
impl DirectRuntime<SimulatedTransport> {
    pub(super) fn new_simulated(
        driver: &DriverLimits,
        address: SocketAddr,
        handle: SimulatedTransportHandle,
        now: Moment,
    ) -> io::Result<Self> {
        let broker = BrokerLimits::default();
        let plan = BorneraLanePlan::new(
            BrokerAddresses::Direct(address),
            broker,
            None,
            KafkaSessionPlan::new(None, broker),
            Box::new(SimulatedAttempt::new(driver, broker, handle)),
        );
        start(driver, plan, now)
    }
}

#[cfg(feature = "tls-rustls")]
impl DirectRuntime<DirectRustlsTransport> {
    pub(in crate::reactor) fn new(
        driver: &DriverLimits,
        address: SocketAddr,
        tls: crate::config::TlsClientConfig,
        sasl: Option<SaslConfig>,
        client_id: Option<ClientId>,
        now: Moment,
    ) -> io::Result<Self> {
        let broker = BrokerLimits::default();
        let plan = BorneraLanePlan::rustls(
            driver,
            broker,
            BrokerAddresses::Direct(address),
            tls,
            sasl,
            client_id,
        );
        start(driver, plan, now)
    }
}

fn start<T: RegisteredTransport>(
    driver: &DriverLimits,
    plan: BorneraLanePlan<T>,
    now: Moment,
) -> io::Result<DirectRuntime<T>> {
    let mut connections = DirectSetOwner::new(driver, DirectSetBounds::direct())?;
    let mut identities = BorneraIdentityAllocator::new();
    let (_, [owner]) = identities
        .reserve_endpoint_lanes::<1>()
        .map_err(io::Error::other)?;
    let lane = start_lane(&mut connections, driver, plan, owner, now)?;
    Ok(DirectRuntime { connections, lane })
}
