//! Explicit bounds for one broker's API version advertisement and retained overlap.

use std::num::NonZeroUsize;

use kafka_wire_core::DecodeLimits;

const DEFAULT_MAX_ADVERTISED_APIS: NonZeroUsize = nonzero(256);
const DEFAULT_MAX_NEGOTIATED_APIS: NonZeroUsize = nonzero(128);

/// Count limits applied while interpreting one `ApiVersions` response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NegotiationLimits {
    max_advertised_apis: NonZeroUsize,
    max_negotiated_apis: NonZeroUsize,
}

impl NegotiationLimits {
    pub(crate) const fn new(
        max_advertised_apis: NonZeroUsize,
        max_negotiated_apis: NonZeroUsize,
    ) -> Self {
        Self {
            max_advertised_apis,
            max_negotiated_apis,
        }
    }

    pub(crate) const fn max_advertised_apis(self) -> usize {
        self.max_advertised_apis.get()
    }

    pub(crate) const fn max_negotiated_apis(self) -> NonZeroUsize {
        self.max_negotiated_apis
    }

    pub(crate) fn decode_limits(self) -> DecodeLimits {
        let mut limits = DecodeLimits::default();
        limits.max_array_elements = self.max_advertised_apis();
        limits
    }
}

impl Default for NegotiationLimits {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ADVERTISED_APIS, DEFAULT_MAX_NEGOTIATED_APIS)
    }
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("negotiation defaults must be nonzero");
    };
    value
}
