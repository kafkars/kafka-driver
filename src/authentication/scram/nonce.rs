//! Validated client nonce generation and server-extension checks.

use std::fmt;

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use kafka_driver_core::AuthenticationFailure;
use ring::rand::{SecureRandom as _, SystemRandom};
use zeroize::Zeroizing;

use super::limits::ScramLimits;

/// One printable, comma-free nonce hidden from diagnostics.
pub(super) struct ScramNonce(Zeroizing<String>);

impl ScramNonce {
    pub(super) fn generate(limits: ScramLimits) -> Result<Self, AuthenticationFailure> {
        let mut entropy = Zeroizing::new([0_u8; 24]);
        SystemRandom::new()
            .fill(entropy.as_mut())
            .map_err(|_| AuthenticationFailure::Protocol)?;
        Self::new(STANDARD_NO_PAD.encode(entropy.as_ref()), limits)
    }

    pub(super) fn new(
        nonce: impl Into<String>,
        limits: ScramLimits,
    ) -> Result<Self, AuthenticationFailure> {
        let nonce = Zeroizing::new(nonce.into());
        validate_nonce(&nonce, limits)?;
        Ok(Self(nonce))
    }

    pub(super) fn validate_server(
        &self,
        server_nonce: &str,
        limits: ScramLimits,
    ) -> Result<(), AuthenticationFailure> {
        validate_nonce(server_nonce, limits)?;
        if server_nonce.len() <= self.0.len() || !server_nonce.starts_with(self.as_str()) {
            return Err(AuthenticationFailure::Malformed);
        }
        Ok(())
    }

    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ScramNonce {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ScramNonce(..)")
    }
}

fn validate_nonce(nonce: &str, limits: ScramLimits) -> Result<(), AuthenticationFailure> {
    if nonce.is_empty()
        || !nonce
            .bytes()
            .all(|byte| (0x21..=0x7e).contains(&byte) && byte != b',')
    {
        return Err(AuthenticationFailure::Malformed);
    }
    if nonce.len() > limits.max_nonce_bytes() {
        return Err(AuthenticationFailure::Capacity);
    }
    Ok(())
}
