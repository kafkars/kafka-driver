//! Bounds retained by one transport-independent Kafka session.

use std::num::NonZeroUsize;

const DEFAULT_MAX_CAPABILITIES: NonZeroUsize = nonzero(128);

/// Resource bounds for negotiated Kafka session policy.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaSessionLimits {
    capabilities: NonZeroUsize,
}

impl KafkaSessionLimits {
    /// Creates a session bound from the maximum retained negotiated APIs.
    pub const fn new(max_capabilities: NonZeroUsize) -> Self {
        Self {
            capabilities: max_capabilities,
        }
    }

    /// Returns the maximum negotiated APIs retained for one session.
    pub const fn max_capabilities(self) -> NonZeroUsize {
        self.capabilities
    }
}

impl Default for KafkaSessionLimits {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_CAPABILITIES)
    }
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("session defaults must be nonzero");
    };
    value
}
