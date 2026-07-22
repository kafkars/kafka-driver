//! Configured bootstrap endpoint admission bound.

use std::num::NonZeroUsize;

const DEFAULT_MAX_ENDPOINTS: NonZeroUsize = nonzero(16);

/// Maximum endpoints inspected while building one bootstrap set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BootstrapLimits {
    max_endpoints: NonZeroUsize,
}

impl BootstrapLimits {
    /// Creates a bootstrap bound from an explicit endpoint count.
    pub const fn new(max_endpoints: NonZeroUsize) -> Self {
        Self { max_endpoints }
    }

    /// Returns the maximum configured endpoint count.
    pub const fn max_endpoints(self) -> NonZeroUsize {
        self.max_endpoints
    }
}

impl Default for BootstrapLimits {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ENDPOINTS)
    }
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("bootstrap defaults must be nonzero");
    };
    value
}
