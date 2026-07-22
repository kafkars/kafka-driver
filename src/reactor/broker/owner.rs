//! One broker endpoint joining connection policy to one registered transport.

use std::net::SocketAddr;

use kafka_driver_core::{
    ConnectionEffect, ConnectionEpoch, ConnectionInput, ConnectionMachine, ConnectionState,
};

use crate::reactor::{
    PollEvent, Poller,
    plaintext::ConnectProgress,
    resource::{PlaintextResources, ResourceIdentity, ResourceToken},
};

use super::{
    BrokerError, BrokerIds,
    failure::{open_failure, plaintext_failure},
    limits::BrokerLimits,
};

/// Single-owner adapter for one broker connection epoch.
#[derive(Debug)]
pub(in crate::reactor) struct SingleBroker {
    address: SocketAddr,
    machine: ConnectionMachine,
    ids: BrokerIds,
    resources: PlaintextResources,
    resource_token: Option<ResourceToken>,
}

impl SingleBroker {
    pub(in crate::reactor) fn new(address: SocketAddr, limits: BrokerLimits) -> Self {
        Self {
            address,
            machine: ConnectionMachine::new(ConnectionEpoch::from_raw(1), limits.connection()),
            ids: BrokerIds::new(),
            resources: PlaintextResources::new(limits.resource_capacity(), limits.plaintext()),
            resource_token: None,
        }
    }

    pub(in crate::reactor) fn start(&mut self, poller: &Poller) -> Result<(), BrokerError> {
        let Some(open) = self.ids.reserve_open() else {
            return Err(BrokerError::IdentityExhausted);
        };
        let transition = self.machine.apply(ConnectionInput::Start {
            effect_id: open.effect_id,
            transport_id: open.transport_id,
        })?;
        let effects = transition.into_effects();
        if effects.len() != 1 {
            return Err(unexpected_or_missing(&effects));
        }
        let ConnectionEffect::OpenTransport {
            epoch,
            effect_id,
            transport_id,
        } = effects[0]
        else {
            return Err(BrokerError::UnexpectedEffect(effects[0]));
        };
        let identity = ResourceIdentity::new(transport_id, epoch);
        match self.resources.open(poller, identity, self.address) {
            Ok(token) => self.resource_token = Some(token),
            Err(error) => {
                self.apply_open_failed(epoch, effect_id, transport_id, open_failure(&error))?;
            }
        }
        Ok(())
    }

    pub(in crate::reactor) fn observe(
        &mut self,
        poller: &Poller,
        event: PollEvent,
    ) -> Result<bool, BrokerError> {
        let PollEvent::Resource { token, .. } = event else {
            return Ok(false);
        };
        if self.resource_token != Some(token) {
            return Ok(false);
        }
        let Some((identity, connection)) = self.resources.get_mut(token) else {
            return Ok(false);
        };
        let progress = connection.finish_connect();
        match progress {
            Ok(ConnectProgress::Opened | ConnectProgress::AlreadyOpen) => {
                let ConnectionState::Opening { effect_id, .. } = self.machine.state() else {
                    return Ok(false);
                };
                self.apply_opened(identity, effect_id)?;
                Ok(true)
            }
            Ok(ConnectProgress::Pending) => Ok(false),
            Err(error) => {
                let ConnectionState::Opening { effect_id, .. } = self.machine.state() else {
                    return Ok(false);
                };
                self.apply_open_failed(
                    identity.epoch(),
                    effect_id,
                    identity.transport_id(),
                    plaintext_failure(&error),
                )?;
                self.close_resource(poller, identity)?;
                Ok(true)
            }
        }
    }

    pub(in crate::reactor) fn state(&self) -> ConnectionState {
        self.machine.state()
    }

    fn apply_opened(
        &mut self,
        identity: ResourceIdentity,
        effect_id: kafka_driver_core::EffectId,
    ) -> Result<(), BrokerError> {
        let transition = self.machine.apply(ConnectionInput::TransportOpened {
            epoch: identity.epoch(),
            effect_id,
            transport_id: identity.transport_id(),
        })?;
        expect_no_effects(&transition.into_effects())
    }

    fn apply_open_failed(
        &mut self,
        epoch: ConnectionEpoch,
        effect_id: kafka_driver_core::EffectId,
        transport_id: kafka_driver_core::TransportId,
        failure: kafka_driver_core::TransportFailure,
    ) -> Result<(), BrokerError> {
        let transition = self.machine.apply(ConnectionInput::TransportOpenFailed {
            epoch,
            effect_id,
            transport_id,
            failure,
        })?;
        expect_no_effects(&transition.into_effects())
    }

    fn close_resource(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
    ) -> Result<(), BrokerError> {
        self.resource_token = None;
        self.resources
            .close(poller, identity)
            .map(|_| ())
            .map_err(BrokerError::ResourceClose)
    }
}

fn expect_no_effects(effects: &[ConnectionEffect]) -> Result<(), BrokerError> {
    match effects.first().copied() {
        Some(effect) => Err(BrokerError::UnexpectedEffect(effect)),
        None => Ok(()),
    }
}

fn unexpected_or_missing(effects: &[ConnectionEffect]) -> BrokerError {
    effects
        .first()
        .copied()
        .map_or(BrokerError::MissingEffect, BrokerError::UnexpectedEffect)
}
