//! Capped exponential retry ownership after complete bootstrap DNS exhaustion.

use std::{error::Error, fmt};

use crate::{BackoffPolicy, RetryOrdinal};

use super::{
    BootstrapRetryDisposition, BootstrapRetryEffect, BootstrapRetryInput, BootstrapRetryState,
    BootstrapRetryTransition,
};

/// Deterministic owner of the delay between complete bootstrap DNS passes.
#[must_use]
#[derive(Debug)]
pub struct BootstrapRetryMachine {
    policy: BackoffPolicy,
    state: BootstrapRetryState,
}

impl BootstrapRetryMachine {
    /// Creates ready retry policy at the first bounded ordinal.
    pub const fn new(policy: BackoffPolicy) -> Self {
        Self {
            policy,
            state: BootstrapRetryState::Ready {
                retry: RetryOrdinal::first(),
            },
        }
    }

    /// Applies one exhaustion, clock, or success observation.
    pub fn apply(
        &mut self,
        input: BootstrapRetryInput,
    ) -> Result<BootstrapRetryTransition, BootstrapRetryError> {
        match input {
            BootstrapRetryInput::Exhausted { now, jitter } => {
                let BootstrapRetryState::Ready { retry } = self.state else {
                    return Ok(ignored());
                };
                let at = now
                    .checked_add(self.policy.delay(retry, jitter))
                    .ok_or(BootstrapRetryError::DeadlineOverflow)?;
                self.state = BootstrapRetryState::Waiting { retry, at };
                Ok(applied(BootstrapRetryEffect::WaitUntil { at }))
            }
            BootstrapRetryInput::Elapsed { now } => Ok(self.elapsed(now)),
            BootstrapRetryInput::Succeeded => {
                self.state = BootstrapRetryState::Ready {
                    retry: RetryOrdinal::first(),
                };
                Ok(BootstrapRetryTransition::new(
                    Vec::new(),
                    BootstrapRetryDisposition::Applied,
                ))
            }
        }
    }

    /// Returns the currently owned wake deadline, if a retry is waiting.
    pub const fn deadline(&self) -> Option<crate::Moment> {
        match self.state {
            BootstrapRetryState::Ready { .. } => None,
            BootstrapRetryState::Waiting { at, .. } => Some(at),
        }
    }

    /// Returns current retry ownership for diagnostics and deterministic tests.
    pub const fn state(&self) -> BootstrapRetryState {
        self.state
    }

    fn elapsed(&mut self, now: crate::Moment) -> BootstrapRetryTransition {
        let BootstrapRetryState::Waiting { retry, at } = self.state else {
            return ignored();
        };
        if now < at {
            return applied(BootstrapRetryEffect::WaitUntil { at });
        }
        self.state = BootstrapRetryState::Ready {
            retry: retry.next().unwrap_or(retry),
        };
        applied(BootstrapRetryEffect::Restart)
    }
}

/// Why a bounded bootstrap retry deadline could not be represented.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapRetryError {
    /// The driver-relative clock cannot represent the computed deadline.
    DeadlineOverflow,
}

impl fmt::Display for BootstrapRetryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bootstrap retry deadline exceeds the driver clock domain")
    }
}

impl Error for BootstrapRetryError {}

fn applied(effect: BootstrapRetryEffect) -> BootstrapRetryTransition {
    BootstrapRetryTransition::new(vec![effect], BootstrapRetryDisposition::Applied)
}

const fn ignored() -> BootstrapRetryTransition {
    BootstrapRetryTransition::new(Vec::new(), BootstrapRetryDisposition::Ignored)
}
