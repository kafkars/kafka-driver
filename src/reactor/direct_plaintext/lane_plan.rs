//! Consumed transport and Kafka policy for one Bornera lane installation.

use std::io;

use bornera::{RegisteredTransport, TcpTransport};
use kafka_driver_core::{AuthenticationPolicy, KafkaSessionLimits, KafkaSessionMachine};
use kafka_wire::{KafkaRequest, SaslAuthenticateRequest, SaslHandshakeRequest};

use crate::{
    authentication::AuthenticationSession,
    config::{BrokerAddresses, ClientId, DriverLimits, SaslConfig},
    reactor::broker::BrokerLimits,
};

use super::attempt::{DirectConnectionAttempt, PlaintextAttempt};
#[cfg(feature = "tls-rustls")]
use super::{attempt::RustlsAttempt, rustls_transport::DirectRustlsTransport};

/// Complete replayable policy consumed when a lane joins one shared set.
pub(in crate::reactor) struct BorneraLanePlan<T: RegisteredTransport> {
    pub(super) addresses: BrokerAddresses,
    pub(super) broker: BrokerLimits,
    pub(super) client_id: Option<ClientId>,
    pub(super) session: KafkaSessionPlan,
    pub(super) connection: Box<dyn DirectConnectionAttempt<T>>,
}

impl<T: RegisteredTransport> BorneraLanePlan<T> {
    pub(super) const fn new(
        addresses: BrokerAddresses,
        broker: BrokerLimits,
        client_id: Option<ClientId>,
        session: KafkaSessionPlan,
        connection: Box<dyn DirectConnectionAttempt<T>>,
    ) -> Self {
        Self {
            addresses,
            broker,
            client_id,
            session,
            connection,
        }
    }
}

impl BorneraLanePlan<TcpTransport> {
    pub(in crate::reactor) fn plaintext(
        driver: &DriverLimits,
        broker: BrokerLimits,
        addresses: BrokerAddresses,
        sasl: Option<SaslConfig>,
        client_id: Option<ClientId>,
    ) -> Self {
        Self::new(
            addresses,
            broker,
            client_id,
            KafkaSessionPlan::new(sasl, broker),
            Box::new(PlaintextAttempt::new(driver, broker)),
        )
    }
}

#[cfg(feature = "tls-rustls")]
impl BorneraLanePlan<DirectRustlsTransport> {
    pub(in crate::reactor) fn rustls(
        driver: &DriverLimits,
        broker: BrokerLimits,
        addresses: BrokerAddresses,
        tls: crate::config::TlsClientConfig,
        sasl: Option<SaslConfig>,
        client_id: Option<ClientId>,
    ) -> Self {
        Self::new(
            addresses,
            broker,
            client_id,
            KafkaSessionPlan::new(sasl, broker),
            Box::new(RustlsAttempt::new(driver, broker, tls)),
        )
    }
}

/// Replayable Kafka session and authentication policy for each fresh epoch.
pub(super) struct KafkaSessionPlan {
    sasl: Option<SaslConfig>,
    broker: BrokerLimits,
}

impl KafkaSessionPlan {
    pub(super) const fn new(sasl: Option<SaslConfig>, broker: BrokerLimits) -> Self {
        Self { sasl, broker }
    }

    pub(super) fn start(&self) -> io::Result<KafkaSessionOwnership> {
        let Some(sasl) = self.sasl.clone() else {
            return Ok(KafkaSessionOwnership {
                machine: KafkaSessionMachine::new(KafkaSessionLimits::default()),
                authentication: None,
            });
        };
        let policy = AuthenticationPolicy::new(
            sasl.mechanism(),
            SaslHandshakeRequest::API_KEY,
            SaslAuthenticateRequest::API_KEY,
            self.broker.authentication(),
        );
        let authentication = AuthenticationSession::new(sasl).map_err(|error| {
            io::Error::other(format!(
                "Bornera authentication session could not start: {error:?}"
            ))
        })?;
        Ok(KafkaSessionOwnership {
            machine: KafkaSessionMachine::new_authenticated(KafkaSessionLimits::default(), policy),
            authentication: Some(authentication),
        })
    }
}

pub(super) struct KafkaSessionOwnership {
    pub(super) machine: KafkaSessionMachine,
    pub(super) authentication: Option<AuthenticationSession>,
}
