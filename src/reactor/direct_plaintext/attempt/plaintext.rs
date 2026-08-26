//! Plain TCP acquisition with fresh framing ownership for every epoch.

use std::net::SocketAddr;

use bornera::{ConnectionToken, TcpTransport, TransportLimits};
use bornera_core::ConnectionEpoch;
use calandria::RetainedBytes;
use kafka_driver_core::Moment;

use super::{
    DirectConnectError, DirectConnectionAttempt, connection_config, plaintext_connect_error,
};
use crate::{
    config::DriverLimits,
    reactor::{bornera::KafkaReplyClassifier, broker::BrokerLimits},
};

use crate::reactor::direct_plaintext::{limits::slot_limits, owner::DirectSet};

pub(in crate::reactor::direct_plaintext) struct PlaintextAttempt {
    driver: DriverLimits,
    broker: BrokerLimits,
    address: SocketAddr,
}

impl PlaintextAttempt {
    pub(in crate::reactor::direct_plaintext) const fn new(
        driver: &DriverLimits,
        broker: BrokerLimits,
        address: SocketAddr,
    ) -> Self {
        Self {
            driver: *driver,
            broker,
            address,
        }
    }
}

impl DirectConnectionAttempt<TcpTransport> for PlaintextAttempt {
    fn connect(
        &self,
        set: &mut DirectSet<TcpTransport>,
        epoch: ConnectionEpoch,
        now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        let (decoder, slot) = slot_limits(
            &self.driver,
            self.broker,
            TransportLimits::new(RetainedBytes::ZERO),
            None,
        )
        .map_err(DirectConnectError::fatal)?;
        set.connect(
            connection_config(self.address, epoch, now, self.broker)
                .map_err(DirectConnectError::fatal)?,
            slot,
            decoder,
            KafkaReplyClassifier,
        )
        .map_err(plaintext_connect_error)
    }
}
