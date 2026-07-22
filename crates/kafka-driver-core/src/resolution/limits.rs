//! Bound on addresses retained from one resolver result.

use std::num::NonZeroUsize;

/// Maximum resolver addresses inspected before rejecting one result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolutionLimits {
    max_addresses: NonZeroUsize,
}

impl ResolutionLimits {
    /// Creates an explicit per-result address bound.
    pub const fn new(max_addresses: NonZeroUsize) -> Self {
        Self { max_addresses }
    }

    /// Returns the maximum address entries inspected from one result.
    pub const fn max_addresses(self) -> NonZeroUsize {
        self.max_addresses
    }
}

impl Default for ResolutionLimits {
    fn default() -> Self {
        Self::new(default_max_addresses())
    }
}

const fn default_max_addresses() -> NonZeroUsize {
    let Some(limit) = NonZeroUsize::new(16) else {
        panic!("resolution defaults must be nonzero");
    };
    limit
}
