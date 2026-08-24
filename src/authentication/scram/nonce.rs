//! System randomness adapted to the library's injected nonce-source boundary.

use ring::rand::{SecureRandom as _, SystemRandom};
use sasl_scram::{NonceError, NonceSource};

pub(super) struct SecureNonceSource(SystemRandom);

impl SecureNonceSource {
    pub(super) fn new() -> Self {
        Self(SystemRandom::new())
    }
}

impl NonceSource for SecureNonceSource {
    fn fill(&mut self, output: &mut [u8]) -> Result<(), NonceError> {
        self.0.fill(output).map_err(|_| NonceError::unavailable())
    }
}

#[cfg(test)]
pub(super) struct FixedNonceSource(Option<[u8; 15]>);

#[cfg(test)]
impl FixedNonceSource {
    pub(super) const fn new(entropy: [u8; 15]) -> Self {
        Self(Some(entropy))
    }
}

#[cfg(test)]
impl NonceSource for FixedNonceSource {
    fn fill(&mut self, output: &mut [u8]) -> Result<(), NonceError> {
        let Some(entropy) = self.0.take() else {
            return Err(NonceError::unavailable());
        };
        if output.len() != entropy.len() {
            return Err(NonceError::unavailable());
        }
        output.copy_from_slice(&entropy);
        Ok(())
    }
}
