//! Rustls acquisition with a fresh decoder feedback gate for every epoch.

use std::{io, net::SocketAddr};

use bornera::ConnectionToken;
use bornera_core::ConnectionEpoch;
use bornera_rustls::RustlsConnector;
use kafka_driver_core::Moment;

use super::{DirectConnectionAttempt, connection_config};
use crate::{
    config::{DriverLimits, TlsClientConfig},
    reactor::{bornera::KafkaReplyClassifier, broker::BrokerLimits},
};

use crate::reactor::direct_plaintext::{
    decoder_gate::DecoderGate,
    limits::{rustls_transport_limits, slot_limits},
    owner::{DirectSet, message},
    rustls_transport::{DirectRustlsConnector, DirectRustlsTransport},
};

pub(in crate::reactor::direct_plaintext) struct RustlsAttempt {
    driver: DriverLimits,
    broker: BrokerLimits,
    address: SocketAddr,
    tls: TlsClientConfig,
}

impl RustlsAttempt {
    pub(in crate::reactor::direct_plaintext) const fn new(
        driver: &DriverLimits,
        broker: BrokerLimits,
        address: SocketAddr,
        tls: TlsClientConfig,
    ) -> Self {
        Self {
            driver: *driver,
            broker,
            address,
            tls,
        }
    }
}

impl DirectConnectionAttempt<DirectRustlsTransport> for RustlsAttempt {
    fn connect(
        &self,
        set: &mut DirectSet<DirectRustlsTransport>,
        epoch: ConnectionEpoch,
        now: Moment,
    ) -> io::Result<ConnectionToken> {
        let transport = rustls_transport_limits()?;
        let decoder_gate = DecoderGate::new();
        let (decoder, slot) = slot_limits(
            &self.driver,
            self.broker,
            transport.transport_limits(),
            Some(decoder_gate.clone()),
        )?;
        let connector = DirectRustlsConnector::new(
            RustlsConnector::new(self.tls.clone().into_bornera(transport)),
            decoder_gate,
        );
        set.connect_with(
            connection_config(self.address, epoch, now, self.broker)?,
            slot,
            decoder,
            KafkaReplyClassifier,
            connector,
        )
        .map_err(message)
    }
}
