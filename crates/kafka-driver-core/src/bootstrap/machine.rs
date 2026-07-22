//! Stable endpoint rotation and identity-fenced DNS outcome ownership.

use crate::DnsRequest;

use super::{
    BootstrapCursor, BootstrapDisposition, BootstrapEffect, BootstrapInput, BootstrapSet,
    BootstrapState, BootstrapTransition,
};

/// Deterministic owner of one bounded pass through configured bootstrap endpoints.
#[must_use]
#[derive(Debug)]
pub struct BootstrapMachine {
    endpoints: BootstrapSet,
    cursor: BootstrapCursor,
    state: BootstrapState,
}

impl BootstrapMachine {
    /// Creates dormant policy over validated nonempty bootstrap membership.
    pub fn new(endpoints: BootstrapSet) -> Self {
        Self {
            endpoints,
            cursor: BootstrapCursor::default(),
            state: BootstrapState::Dormant,
        }
    }

    /// Applies one command or external resolver outcome.
    #[must_use = "bootstrap effects must be interpreted in order"]
    pub fn apply(&mut self, input: BootstrapInput) -> BootstrapTransition {
        match input {
            BootstrapInput::Start { epoch, effect_id } => self.start(epoch, effect_id),
            BootstrapInput::ResolutionCompleted {
                outcome,
                retry_effect_id,
            } => self.complete(outcome, retry_effect_id),
        }
    }

    /// Returns current bootstrap ownership.
    pub const fn state(&self) -> &BootstrapState {
        &self.state
    }

    fn start(
        &mut self,
        epoch: crate::ConnectionEpoch,
        effect_id: crate::EffectId,
    ) -> BootstrapTransition {
        if matches!(self.state, BootstrapState::Resolving { .. }) {
            return BootstrapTransition::new(Vec::new(), BootstrapDisposition::IgnoredBusy);
        }
        let endpoint = self.cursor.select_next(&self.endpoints).clone();
        self.state = BootstrapState::Resolving {
            epoch,
            effect_id,
            endpoint: endpoint.clone(),
            remaining: self.endpoints.len() - 1,
        };
        BootstrapTransition::new(
            vec![BootstrapEffect::Resolve {
                request: DnsRequest::new(epoch, effect_id, endpoint),
            }],
            BootstrapDisposition::Applied,
        )
    }

    fn complete(
        &mut self,
        outcome: crate::DnsOutcome,
        retry_effect_id: crate::EffectId,
    ) -> BootstrapTransition {
        let BootstrapState::Resolving {
            epoch,
            effect_id,
            endpoint,
            remaining,
        } = &self.state
        else {
            return BootstrapTransition::new(Vec::new(), BootstrapDisposition::IgnoredStale);
        };
        if outcome.epoch() != *epoch || outcome.effect_id() != *effect_id {
            return BootstrapTransition::new(Vec::new(), BootstrapDisposition::IgnoredStale);
        }
        let epoch = *epoch;
        let endpoint = endpoint.clone();
        let remaining = *remaining;
        match outcome.into_result() {
            Ok(addresses) => {
                self.state = BootstrapState::Resolved { epoch };
                BootstrapTransition::new(
                    vec![BootstrapEffect::Resolved {
                        epoch,
                        endpoint,
                        addresses,
                    }],
                    BootstrapDisposition::Applied,
                )
            }
            Err(last_failure) if remaining == 0 => {
                self.state = BootstrapState::Exhausted {
                    epoch,
                    last_failure,
                };
                BootstrapTransition::new(
                    vec![BootstrapEffect::Exhausted {
                        epoch,
                        last_failure,
                    }],
                    BootstrapDisposition::Applied,
                )
            }
            Err(_) => self.retry(epoch, retry_effect_id, remaining - 1),
        }
    }

    fn retry(
        &mut self,
        epoch: crate::ConnectionEpoch,
        effect_id: crate::EffectId,
        remaining: usize,
    ) -> BootstrapTransition {
        let endpoint = self.cursor.select_next(&self.endpoints).clone();
        self.state = BootstrapState::Resolving {
            epoch,
            effect_id,
            endpoint: endpoint.clone(),
            remaining,
        };
        BootstrapTransition::new(
            vec![BootstrapEffect::Resolve {
                request: DnsRequest::new(epoch, effect_id, endpoint),
            }],
            BootstrapDisposition::Applied,
        )
    }
}
