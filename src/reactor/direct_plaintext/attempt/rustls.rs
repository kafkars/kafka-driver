//! Rustls acquisition with a fresh decoder feedback gate for every epoch.

use std::net::SocketAddr;

use bornera::{ConnectError, ConnectionToken};
use bornera_core::ConnectionEpoch;
use bornera_rustls::RustlsConnector;
use kafka_driver_core::Moment;

use super::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt, connection_config};
use crate::{
    config::{DriverLimits, TlsClientConfig},
    reactor::{bornera::KafkaReplyClassifier, broker::BrokerLimits},
};

use crate::reactor::direct_plaintext::{
    decoder_gate::DecoderGate,
    limits::{rustls_transport_limits, slot_limits},
    owner::DirectSet,
    rustls_transport::{DirectRustlsConnector, DirectRustlsTransport},
};

pub(in crate::reactor::direct_plaintext) struct RustlsAttempt {
    driver: DriverLimits,
    broker: BrokerLimits,
    tls: TlsClientConfig,
}

impl RustlsAttempt {
    pub(in crate::reactor::direct_plaintext) const fn new(
        driver: &DriverLimits,
        broker: BrokerLimits,
        tls: TlsClientConfig,
    ) -> Self {
        Self {
            driver: *driver,
            broker,
            tls,
        }
    }

    #[cfg(test)]
    pub(in crate::reactor::direct_plaintext) const fn server_name_for_test(
        &self,
    ) -> &rustls::pki_types::ServerName<'static> {
        self.tls.server_name_for_test()
    }
}

impl DirectConnectionAttempt<DirectRustlsTransport> for RustlsAttempt {
    fn connect(
        &self,
        set: &mut DirectSet<DirectRustlsTransport>,
        owner: BorneraLaneOwner,
        address: SocketAddr,
        epoch: ConnectionEpoch,
        now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        let transport = rustls_transport_limits().map_err(DirectConnectError::fatal)?;
        let decoder_gate = DecoderGate::new();
        let (decoder, slot) = slot_limits(
            &self.driver,
            self.broker,
            transport.transport_limits(),
            Some(decoder_gate.clone()),
        )
        .map_err(DirectConnectError::fatal)?;
        let connector = DirectRustlsConnector::new(
            RustlsConnector::new(self.tls.clone().into_bornera(transport)),
            decoder_gate,
        );
        set.connect_with(
            connection_config(owner, address, epoch, now, self.broker)
                .map_err(DirectConnectError::fatal)?,
            slot,
            decoder,
            KafkaReplyClassifier,
            connector,
        )
        .map_err(rustls_connect_error)
    }
}

pub(in crate::reactor::direct_plaintext) fn rustls_connect_error<E: std::fmt::Display>(
    error: ConnectError<E>,
) -> DirectConnectError {
    match error {
        ConnectError::Io(source)
            if source
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<bornera_rustls::RustlsConnectError>())
                .is_none() =>
        {
            DirectConnectError::endpoint(source)
        }
        other => DirectConnectError::fatal(other),
    }
}
