//! Bootstrap-machine ownership and conversion of resolved addresses into broker policy.

use kafka_driver_core::{
    BackoffPolicy, BootstrapEffect, BootstrapInput, BootstrapMachine, BootstrapRetryEffect,
    BootstrapRetryInput, BootstrapRetryMachine, ConnectionEpoch, DnsOutcome, EffectId,
    JitterSample, Moment,
};

use crate::config::{BootstrapConfig, BrokerConfig, BrokerTemplate};

use super::BootstrapOwnerError;

/// One external action returned to the shard-owned resolver interpreter.
pub(in crate::reactor) enum BootstrapAction {
    Resolve(kafka_driver_core::DnsRequest),
    Install(BrokerConfig),
    RetryScheduled,
}

/// Reactor-side owner joining pure bootstrap policy to the blocking DNS worker.
#[derive(Debug)]
pub(in crate::reactor) struct BootstrapOwner {
    machine: BootstrapMachine,
    retry: BootstrapRetryMachine,
    broker: BrokerTemplate,
    next_epoch: Option<u64>,
}

impl BootstrapOwner {
    pub(in crate::reactor) fn start(
        config: BootstrapConfig,
        effect_id: EffectId,
    ) -> Result<(Self, kafka_driver_core::DnsRequest), BootstrapOwnerError> {
        let (endpoints, broker) = config.into_parts();
        let mut owner = Self {
            machine: BootstrapMachine::new(endpoints),
            retry: BootstrapRetryMachine::new(BackoffPolicy::default()),
            broker,
            next_epoch: Some(2),
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
        now: Moment,
        jitter: JitterSample,
    ) -> Result<BootstrapAction, BootstrapOwnerError> {
        let transition = self.machine.apply(BootstrapInput::ResolutionCompleted {
            outcome,
            retry_effect_id,
        });
        self.interpret_completion(transition.effects(), now, jitter)
    }

    pub(in crate::reactor) fn restart(
        &mut self,
        effect_id: EffectId,
    ) -> Result<kafka_driver_core::DnsRequest, BootstrapOwnerError> {
        let raw = self.next_epoch.ok_or(BootstrapOwnerError::EpochExhausted)?;
        self.next_epoch = raw.checked_add(1);
        let transition = self.machine.apply(BootstrapInput::Start {
            epoch: ConnectionEpoch::from_raw(raw),
            effect_id,
        });
        let BootstrapAction::Resolve(request) = self.interpret(transition.effects())? else {
            return Err(BootstrapOwnerError::UnexpectedEffect);
        };
        Ok(request)
    }

    pub(in crate::reactor) const fn retry_deadline(&self) -> Option<Moment> {
        self.retry.deadline()
    }

    pub(in crate::reactor) fn retry_elapsed(
        &mut self,
        now: Moment,
        effect_id: EffectId,
    ) -> Result<Option<kafka_driver_core::DnsRequest>, BootstrapOwnerError> {
        let transition = self.retry.apply(BootstrapRetryInput::Elapsed { now })?;
        match transition.effects() {
            [BootstrapRetryEffect::Restart] => self.restart(effect_id).map(Some),
            [BootstrapRetryEffect::WaitUntil { .. }] | [] => Ok(None),
            _ => Err(BootstrapOwnerError::UnexpectedEffect),
        }
    }

    fn interpret_completion(
        &mut self,
        effects: &[BootstrapEffect],
        now: Moment,
        jitter: JitterSample,
    ) -> Result<BootstrapAction, BootstrapOwnerError> {
        if matches!(effects, [BootstrapEffect::Exhausted { .. }]) {
            let transition = self
                .retry
                .apply(BootstrapRetryInput::Exhausted { now, jitter })?;
            return match transition.effects() {
                [BootstrapRetryEffect::WaitUntil { .. }] => Ok(BootstrapAction::RetryScheduled),
                _ => Err(BootstrapOwnerError::UnexpectedEffect),
            };
        }
        self.interpret(effects)
    }

    fn interpret(
        &mut self,
        effects: &[BootstrapEffect],
    ) -> Result<BootstrapAction, BootstrapOwnerError> {
        match effects {
            [BootstrapEffect::Resolve { request }] => Ok(BootstrapAction::Resolve(request.clone())),
            [
                BootstrapEffect::Resolved {
                    endpoint,
                    addresses,
                    ..
                },
            ] => {
                let _ = self.retry.apply(BootstrapRetryInput::Succeeded)?;
                Ok(BootstrapAction::Install(
                    self.broker
                        .clone()
                        .at_resolved(endpoint.clone(), addresses.clone()),
                ))
            }
            _ => Err(BootstrapOwnerError::UnexpectedEffect),
        }
    }
}
