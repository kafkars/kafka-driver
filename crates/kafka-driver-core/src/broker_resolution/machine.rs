//! Route-generation and connection-epoch fencing for advertised broker DNS.

use crate::{BrokerId, BrokerResolutionInput, DnsRequest};

use super::{
    BrokerResolutionDisposition, BrokerResolutionEffect, BrokerResolutionState,
    BrokerResolutionTransition,
};

/// Deterministic DNS policy owned by exactly one Kafka broker identity.
#[must_use]
#[derive(Debug)]
pub struct BrokerResolutionMachine {
    broker_id: BrokerId,
    state: BrokerResolutionState,
}

impl BrokerResolutionMachine {
    /// Creates dormant resolution policy for one broker identity.
    pub const fn new(broker_id: BrokerId) -> Self {
        Self {
            broker_id,
            state: BrokerResolutionState::Dormant,
        }
    }

    /// Applies route demand or one identity-fenced resolver outcome.
    pub fn apply(&mut self, input: BrokerResolutionInput) -> BrokerResolutionTransition {
        match input {
            BrokerResolutionInput::Start {
                route,
                endpoint,
                epoch,
                effect_id,
            } => self.start(route, endpoint, epoch, effect_id),
            BrokerResolutionInput::ResolutionCompleted { outcome } => self.complete(outcome),
        }
    }

    /// Returns current advertised endpoint resolution ownership.
    pub const fn state(&self) -> &BrokerResolutionState {
        &self.state
    }

    fn start(
        &mut self,
        route: crate::BrokerRoute,
        endpoint: crate::BrokerEndpoint,
        epoch: crate::ConnectionEpoch,
        effect_id: crate::EffectId,
    ) -> BrokerResolutionTransition {
        if route.broker_id() != self.broker_id {
            return transition(Vec::new(), BrokerResolutionDisposition::RejectedBroker);
        }
        if let Some((current_route, current_epoch)) = current_identity(&self.state) {
            if route.generation() < current_route.generation() || epoch < current_epoch {
                return stale();
            }
            if epoch == current_epoch {
                return transition(Vec::new(), BrokerResolutionDisposition::IgnoredBusy);
            }
        }
        self.state = BrokerResolutionState::Resolving {
            route,
            endpoint: endpoint.clone(),
            epoch,
            effect_id,
        };
        transition(
            vec![BrokerResolutionEffect::Resolve {
                request: DnsRequest::new(epoch, effect_id, endpoint),
            }],
            BrokerResolutionDisposition::Applied,
        )
    }

    fn complete(&mut self, outcome: crate::DnsOutcome) -> BrokerResolutionTransition {
        let BrokerResolutionState::Resolving {
            route,
            endpoint,
            epoch,
            effect_id,
        } = &self.state
        else {
            return stale();
        };
        if outcome.epoch() != *epoch || outcome.effect_id() != *effect_id {
            return stale();
        }
        let route = *route;
        let endpoint = endpoint.clone();
        let epoch = *epoch;
        match outcome.into_result() {
            Ok(addresses) => {
                self.state = BrokerResolutionState::Resolved { route, epoch };
                transition(
                    vec![BrokerResolutionEffect::Resolved {
                        route,
                        epoch,
                        endpoint,
                        addresses,
                    }],
                    BrokerResolutionDisposition::Applied,
                )
            }
            Err(failure) => {
                self.state = BrokerResolutionState::Failed {
                    route,
                    epoch,
                    failure,
                };
                transition(
                    vec![BrokerResolutionEffect::Failed { route, failure }],
                    BrokerResolutionDisposition::Applied,
                )
            }
        }
    }
}

fn current_identity(
    state: &BrokerResolutionState,
) -> Option<(crate::BrokerRoute, crate::ConnectionEpoch)> {
    match state {
        BrokerResolutionState::Dormant => None,
        BrokerResolutionState::Resolving { route, epoch, .. }
        | BrokerResolutionState::Resolved { route, epoch }
        | BrokerResolutionState::Failed { route, epoch, .. } => Some((*route, *epoch)),
    }
}

fn stale() -> BrokerResolutionTransition {
    transition(Vec::new(), BrokerResolutionDisposition::IgnoredStale)
}

fn transition(
    effects: Vec<BrokerResolutionEffect>,
    disposition: BrokerResolutionDisposition,
) -> BrokerResolutionTransition {
    BrokerResolutionTransition::new(effects, disposition)
}
