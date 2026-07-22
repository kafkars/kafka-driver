//! Explicit bound on challenge-response work for one authentication attempt.

use std::num::NonZeroU8;

const DEFAULT_MAX_EXCHANGE_ROUNDS: NonZeroU8 = nonzero(4);

/// Persistent authentication work bound for one connection epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthenticationLimits {
    max_exchange_rounds: NonZeroU8,
}

impl AuthenticationLimits {
    /// Creates a limit from the maximum client authentication messages.
    pub const fn new(max_exchange_rounds: NonZeroU8) -> Self {
        Self {
            max_exchange_rounds,
        }
    }

    /// Returns the maximum client authentication messages.
    pub const fn max_exchange_rounds(self) -> NonZeroU8 {
        self.max_exchange_rounds
    }
}

impl Default for AuthenticationLimits {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_EXCHANGE_ROUNDS)
    }
}

const fn nonzero(value: u8) -> NonZeroU8 {
    let Some(value) = NonZeroU8::new(value) else {
        panic!("authentication defaults must be nonzero");
    };
    value
}
