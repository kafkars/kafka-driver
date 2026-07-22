//! Bootstrap-machine ownership and conversion of resolved addresses into broker policy.

use kafka_driver_core::{
    BootstrapEffect, BootstrapInput, BootstrapMachine, ConnectionEpoch, DnsOutcome, EffectId,
};

use crate::config::{BootstrapConfig, BrokerConfig, BrokerTemplate};

use super::BootstrapOwnerError;

/// One external action returned to the shard-owned resolver interpreter.
pub(in crate::reactor) enum BootstrapAction {
    Resolve(kafka_driver_core::DnsRequest),
    Install(BrokerConfig),
    Exhausted,
}

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
    ) -> Result<(Self, kafka_driver_core::DnsRequest), BootstrapOwnerError> {
        let (endpoints, broker) = config.into_parts();
        let mut owner = Self {
            machine: BootstrapMachine::new(endpoints),
            broker,
        };
        let transition = owner.machine.apply(BootstrapInput::Start {
            epoch: ConnectionEpoch::from_raw(1),
            effect_id,
        });
        let BootstrapAction::Resolve(request) = owner.interpret(transition.effects())? else {
            return Err(BootstrapOwnerError::UnexpectedEffect);
        };
        Ok((owner, request))
    }

    pub(in crate::reactor) fn complete(
        &mut self,
        outcome: DnsOutcome,
        retry_effect_id: EffectId,
    ) -> Result<BootstrapAction, BootstrapOwnerError> {
        let transition = self.machine.apply(BootstrapInput::ResolutionCompleted {
            outcome,
            retry_effect_id,
        });
        self.interpret(transition.effects())
    }

    fn interpret(
        &self,
        effects: &[BootstrapEffect],
    ) -> Result<BootstrapAction, BootstrapOwnerError> {
        match effects {
            [BootstrapEffect::Exhausted { .. }] => Ok(BootstrapAction::Exhausted),
            [BootstrapEffect::Resolve { request }] => Ok(BootstrapAction::Resolve(request.clone())),
            [BootstrapEffect::Resolved { addresses, .. }] => Ok(BootstrapAction::Install(
                self.broker.clone().at_resolved(addresses.clone()),
            )),
            _ => Err(BootstrapOwnerError::UnexpectedEffect),
        }
    }
}
