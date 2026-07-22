//! Persistent broker directory capacity policy.

use std::num::NonZeroUsize;

const DEFAULT_MAX_BROKERS: NonZeroUsize = nonzero(4_096);

/// Maximum broker entries retained in one metadata generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrokerDirectoryLimits {
    max_brokers: NonZeroUsize,
}

impl BrokerDirectoryLimits {
    /// Returns the reference default broker-membership bound.
    pub const fn defaults() -> Self {
        Self::new(DEFAULT_MAX_BROKERS)
    }

    /// Creates directory limits from an explicit broker count.
    pub const fn new(max_brokers: NonZeroUsize) -> Self {
        Self { max_brokers }
    }

    /// Returns the maximum retained broker count.
    pub const fn max_brokers(self) -> NonZeroUsize {
        self.max_brokers
    }
}

impl Default for BrokerDirectoryLimits {
    fn default() -> Self {
        Self::defaults()
    }
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("broker directory defaults must be nonzero");
    };
    value
}
