//! Persistent set construction with the first replayable connection attempt.

use std::{io, net::SocketAddr};

use bornera::{RegisteredTransport, TcpTransport};
use bornera_core::{ConnectionId, EndpointId, LaneId};
use calandria::TimerOwnerId;
use kafka_driver_core::Moment;

use crate::config::{ClientId, DriverLimits, SaslConfig};
use crate::reactor::broker::BrokerLimits;

#[cfg(feature = "tls-rustls")]
use super::{attempt::RustlsAttempt, rustls_transport::DirectRustlsTransport};
use super::{
    attempt::{DirectConnectionAttempt, DirectConnectionOwner, PlaintextAttempt},
    lane_construction::start_lane,
    limits::DirectSetBounds,
    owner::ID,
    runtime::DirectRuntime,
    session_plan::DirectSessionPlan,
    set_owner::DirectSetOwner,
};

impl DirectRuntime<TcpTransport> {
    pub(in crate::reactor) fn new(
        driver: &DriverLimits,
        address: SocketAddr,
        sasl: Option<SaslConfig>,
        client_id: Option<ClientId>,
        now: Moment,
    ) -> io::Result<Self> {
        let broker = BrokerLimits::default();
        let session_plan = DirectSessionPlan::new(sasl, broker);
        let connection_attempt: Box<dyn DirectConnectionAttempt<TcpTransport>> =
            Box::new(PlaintextAttempt::new(driver, broker, address));
        start(
            driver,
            broker,
            address,
            client_id,
            session_plan,
            connection_attempt,
            now,
        )
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
        start(
            driver,
            broker,
            address,
            None,
            DirectSessionPlan::new(sasl, broker),
            attempt,
            now,
        )
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
        let session_plan = DirectSessionPlan::new(sasl, broker);
        let connection_attempt: Box<dyn DirectConnectionAttempt<DirectRustlsTransport>> =
            Box::new(RustlsAttempt::new(driver, broker, address, tls));
        start(
            driver,
            broker,
            address,
            client_id,
            session_plan,
            connection_attempt,
            now,
        )
    }
}

fn start<T: RegisteredTransport>(
    driver: &DriverLimits,
    broker: BrokerLimits,
    address: SocketAddr,
    client_id: Option<ClientId>,
    session_plan: DirectSessionPlan,
    connection_attempt: Box<dyn DirectConnectionAttempt<T>>,
    now: Moment,
) -> io::Result<DirectRuntime<T>> {
    let mut connections = DirectSetOwner::new(driver, DirectSetBounds::direct())?;
    let connection_owner = DirectConnectionOwner::new(
        EndpointId::new(ID),
        LaneId::new(1),
        ConnectionId::new(ID),
        TimerOwnerId::new(ID),
    );
    let lane = start_lane(
        &mut connections,
        driver,
        broker,
        address,
        client_id,
        session_plan,
        connection_attempt,
        connection_owner,
        now,
    )?;
    Ok(DirectRuntime { connections, lane })
}
