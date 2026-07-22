//! Bootstrap-machine ownership and conversion of resolved addresses into broker policy.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};

use kafka_driver_core::{
    BootstrapEffect, BootstrapInput, BootstrapMachine, ConnectionEpoch, DnsOutcome, IpAddress,
    ResolvedAddress,
};

use crate::config::{BootstrapConfig, BrokerConfig, BrokerTemplate};

use super::{BootstrapOwnerError, identity::BootstrapEffectIds};
use crate::reactor::resolver::Resolver;

/// Reactor-side owner joining pure bootstrap policy to the blocking DNS worker.
#[derive(Debug)]
pub(in crate::reactor) struct BootstrapOwner {
    machine: BootstrapMachine,
    broker: BrokerTemplate,
    effect_ids: BootstrapEffectIds,
}

impl BootstrapOwner {
    pub(super) fn start(
        config: BootstrapConfig,
        resolver: &Resolver,
    ) -> Result<Self, BootstrapOwnerError> {
        let (endpoints, broker) = config.into_parts();
        let mut owner = Self {
            machine: BootstrapMachine::new(endpoints),
            broker,
            effect_ids: BootstrapEffectIds::new(),
        };
        let Some(effect_id) = owner.effect_ids.reserve() else {
            return Err(BootstrapOwnerError::IdentityExhausted);
        };
        let transition = owner.machine.apply(BootstrapInput::Start {
            epoch: ConnectionEpoch::from_raw(1),
            effect_id,
        });
        if owner.interpret(transition.effects(), resolver)?.is_some() {
            return Err(BootstrapOwnerError::UnexpectedEffect);
        }
        Ok(owner)
    }

    pub(super) fn complete(
        &mut self,
        outcome: DnsOutcome,
        resolver: &Resolver,
    ) -> Result<Option<BrokerConfig>, BootstrapOwnerError> {
        let Some(retry_effect_id) = self.effect_ids.reserve() else {
            return Err(BootstrapOwnerError::IdentityExhausted);
        };
        let transition = self.machine.apply(BootstrapInput::ResolutionCompleted {
            outcome,
            retry_effect_id,
        });
        self.interpret(transition.effects(), resolver)
    }

    fn interpret(
        &self,
        effects: &[BootstrapEffect],
        resolver: &Resolver,
    ) -> Result<Option<BrokerConfig>, BootstrapOwnerError> {
        match effects {
            [] | [BootstrapEffect::Exhausted { .. }] => Ok(None),
            [BootstrapEffect::Resolve { request }] => {
                resolver.submit(request.clone())?;
                Ok(None)
            }
            [BootstrapEffect::Resolved { addresses, .. }] => {
                let Some(address) = addresses.iter().next().copied() else {
                    return Err(BootstrapOwnerError::UnexpectedEffect);
                };
                Ok(Some(self.broker.clone().at(socket_address(address))))
            }
            _ => Err(BootstrapOwnerError::UnexpectedEffect),
        }
    }
}

fn socket_address(address: ResolvedAddress) -> SocketAddr {
    match address.ip() {
        IpAddress::V4(octets) => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::from(octets)), address.port().get())
        }
        IpAddress::V6(octets) => SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::from(octets),
            address.port().get(),
            address.flow_info(),
            address.scope_id(),
        )),
    }
}
