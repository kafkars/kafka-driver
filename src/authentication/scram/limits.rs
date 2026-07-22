//! Named CPU, salt, and nonce bounds for an untrusted SCRAM challenge.

/// Fixed safety limits applied before allocating or deriving a SCRAM proof.
#[derive(Clone, Copy, Debug)]
pub(super) struct ScramLimits {
    nonce_bytes: usize,
    salt_bytes: usize,
    iterations: u32,
}

impl ScramLimits {
    pub(super) const fn new(
        max_nonce_bytes: usize,
        max_salt_bytes: usize,
        max_iterations: u32,
    ) -> Self {
        Self {
            nonce_bytes: max_nonce_bytes,
            salt_bytes: max_salt_bytes,
            iterations: max_iterations,
        }
    }

    pub(super) const fn max_nonce_bytes(self) -> usize {
        self.nonce_bytes
    }

    pub(super) const fn max_salt_bytes(self) -> usize {
        self.salt_bytes
    }

    pub(super) const fn max_iterations(self) -> u32 {
        self.iterations
    }
}

impl Default for ScramLimits {
    fn default() -> Self {
        Self::new(256, 1_024, 1_000_000)
    }
}
