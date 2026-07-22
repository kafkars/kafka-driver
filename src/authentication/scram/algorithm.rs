//! Hash-family selection and constant-time server-proof verification.

use std::fmt;

use kafka_driver_core::{AuthenticationFailure, SaslMechanism};
use ring::{digest, hmac, pbkdf2};

/// Cryptographic primitives selected by one public SCRAM mechanism.
#[derive(Clone, Copy)]
pub(super) struct ScramAlgorithm {
    mechanism: SaslMechanism,
    digest: &'static digest::Algorithm,
    hmac: hmac::Algorithm,
    pbkdf2: pbkdf2::Algorithm,
}

impl ScramAlgorithm {
    pub(super) fn for_mechanism(mechanism: SaslMechanism) -> Result<Self, AuthenticationFailure> {
        match mechanism {
            SaslMechanism::ScramSha256 => Ok(Self {
                mechanism,
                digest: &digest::SHA256,
                hmac: hmac::HMAC_SHA256,
                pbkdf2: pbkdf2::PBKDF2_HMAC_SHA256,
            }),
            SaslMechanism::ScramSha512 => Ok(Self {
                mechanism,
                digest: &digest::SHA512,
                hmac: hmac::HMAC_SHA512,
                pbkdf2: pbkdf2::PBKDF2_HMAC_SHA512,
            }),
            SaslMechanism::Plain => Err(AuthenticationFailure::Protocol),
        }
    }

    pub(super) const fn mechanism(self) -> SaslMechanism {
        self.mechanism
    }

    pub(super) fn output_len(self) -> usize {
        self.digest.output_len()
    }

    pub(super) const fn digest(self) -> &'static digest::Algorithm {
        self.digest
    }

    pub(super) const fn hmac(self) -> hmac::Algorithm {
        self.hmac
    }

    pub(super) const fn pbkdf2(self) -> pbkdf2::Algorithm {
        self.pbkdf2
    }

    pub(super) fn verify(
        self,
        server_key: &[u8],
        auth_message: &[u8],
        signature: &[u8],
    ) -> Result<(), AuthenticationFailure> {
        let key = hmac::Key::new(self.hmac, server_key);
        hmac::verify(&key, auth_message, signature)
            .map_err(|_| AuthenticationFailure::InvalidServerProof)
    }
}

impl fmt::Debug for ScramAlgorithm {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.mechanism.fmt(formatter)
    }
}
