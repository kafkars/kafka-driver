//! Bootstrap-machine ownership and conversion of resolved addresses into broker policy.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};

use kafka_driver_core::{
    BootstrapEffect, BootstrapInput, BootstrapMachine, ConnectionEpoch, DnsOutcome, EffectId,
    IpAddress, ResolvedAddress,
};

use crate::config::{BootstrapConfig, BrokerConfig, BrokerTemplate};

use super::BootstrapOwnerError;
use crate::reactor::resolver::Resolver;

/// Reactor-side owner joining pure bootstrap policy to the blocking DNS worker.
#[derive(Debug)]
pub(in crate::reactor) struct BootstrapOwner {
    machine: BootstrapMachine,
    broker: BrokerTemplate,
}

impl BootstrapOwner {
    pub(in crate::reactor) fn start(
        config: BootstrapConfig,
        effect_id: EffectId,
        resolver: &Resolver,
    ) -> Result<Self, BootstrapOwnerError> {
        let (endpoints, broker) = config.into_parts();
        let mut owner = Self {
            machine: BootstrapMachine::new(endpoints),
            broker,
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

    pub(in crate::reactor) fn complete(
        &mut self,
        outcome: DnsOutcome,
        retry_effect_id: EffectId,
        resolver: &Resolver,
    ) -> Result<Option<BrokerConfig>, BootstrapOwnerError> {
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
