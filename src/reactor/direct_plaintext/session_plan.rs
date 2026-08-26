//! Replayable Kafka session and authentication ownership for each fresh epoch.

use std::io;

use kafka_driver_core::{AuthenticationPolicy, KafkaSessionLimits, KafkaSessionMachine};
use kafka_wire::{KafkaRequest, SaslAuthenticateRequest, SaslHandshakeRequest};

use crate::{
    authentication::AuthenticationSession, config::SaslConfig, reactor::broker::BrokerLimits,
};

pub(super) struct DirectSessionPlan {
    sasl: Option<SaslConfig>,
    broker: BrokerLimits,
}

impl DirectSessionPlan {
    pub(super) const fn new(sasl: Option<SaslConfig>, broker: BrokerLimits) -> Self {
        Self { sasl, broker }
    }

    pub(super) fn start(&self) -> io::Result<DirectSessionOwnership> {
        let Some(sasl) = self.sasl.clone() else {
            return Ok(DirectSessionOwnership {
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
                "direct authentication session could not start: {error:?}"
            ))
        })?;
        Ok(DirectSessionOwnership {
            machine: KafkaSessionMachine::new_authenticated(KafkaSessionLimits::default(), policy),
            authentication: Some(authentication),
        })
    }
}

pub(super) struct DirectSessionOwnership {
    pub(super) machine: KafkaSessionMachine,
    pub(super) authentication: Option<AuthenticationSession>,
}
