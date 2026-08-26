//! Replayable transport acquisition for one exact direct connection epoch.

mod plaintext;
#[cfg(feature = "tls-rustls")]
mod rustls;

use std::{io, net::SocketAddr};

use bornera::{ConnectionConfig, ConnectionIdentity, ConnectionToken, RegisteredTransport};
use bornera_core::{ConnectionEpoch, ConnectionId, EndpointId, LaneId};
use calandria::{Deadline, TimerOwnerId};
use kafka_driver_core::Moment;

use super::owner::{DirectSet, ID, calandria_moment};
use crate::reactor::broker::BrokerLimits;

pub(super) use plaintext::PlaintextAttempt;
#[cfg(feature = "tls-rustls")]
pub(super) use rustls::RustlsAttempt;

/// Immutable policy capable of creating a fresh transport and slot per epoch.
pub(super) trait DirectConnectionAttempt<T: RegisteredTransport> {
    fn connect(
        &self,
        set: &mut DirectSet<T>,
        epoch: ConnectionEpoch,
        now: Moment,
    ) -> io::Result<ConnectionToken>;
}

fn connection_config(
    address: SocketAddr,
    epoch: ConnectionEpoch,
    now: Moment,
    broker: BrokerLimits,
) -> io::Result<ConnectionConfig> {
    let connect_deadline = now
        .checked_add(broker.connect_timeout())
        .ok_or_else(|| io::Error::other("direct connect deadline overflowed"))?;
    let lane =
        u32::try_from(ID).map_err(|_| io::Error::other("direct lane identity exceeds u32"))?;
    Ok(ConnectionConfig::new(
        ConnectionIdentity::new(
            EndpointId::new(ID),
            LaneId::new(lane),
            ConnectionId::new(ID),
            epoch,
        ),
        address,
        Deadline::at(calandria_moment(connect_deadline)),
        TimerOwnerId::new(ID),
    ))
}
