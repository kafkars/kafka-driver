//! Independent SCRAM server transcript oracle for both Kafka algorithms.

#![allow(
    dead_code,
    reason = "shared SCRAM fixture capabilities are selected by separate transport targets"
)]

use std::num::NonZeroU32;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use kafka_driver::SaslConfig;
use ring::{digest, hmac, pbkdf2};

const SALT: &[u8] = b"reference-salt";
const ITERATIONS: u32 = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScramAlgorithm {
    Sha256,
    Sha512,
}

impl ScramAlgorithm {
    pub(crate) const fn mechanism(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }

    pub(crate) fn configuration(self, username: &str, password: &str) -> SaslConfig {
        let result = match self {
            Self::Sha256 => SaslConfig::scram_sha_256(username, password),
            Self::Sha512 => SaslConfig::scram_sha_512(username, password),
        };
        result.unwrap_or_else(|error| panic!("construct {} config: {error}", self.mechanism()))
    }

    const fn output_bytes(self) -> usize {
        match self {
            Self::Sha256 => 32,
            Self::Sha512 => 64,
        }
    }

    const fn pbkdf2(self) -> pbkdf2::Algorithm {
        match self {
            Self::Sha256 => pbkdf2::PBKDF2_HMAC_SHA256,
            Self::Sha512 => pbkdf2::PBKDF2_HMAC_SHA512,
        }
    }

    const fn digest(self) -> &'static digest::Algorithm {
        match self {
            Self::Sha256 => &digest::SHA256,
            Self::Sha512 => &digest::SHA512,
        }
    }

    const fn hmac(self) -> hmac::Algorithm {
        match self {
            Self::Sha256 => hmac::HMAC_SHA256,
            Self::Sha512 => hmac::HMAC_SHA512,
        }
    }
}

pub(crate) struct ScramTranscript {
    server_first: Vec<u8>,
    client_final: Vec<u8>,
    server_final: Vec<u8>,
    invalid_server_final: Vec<u8>,
}

impl ScramTranscript {
    pub(crate) fn from_client_first(
        algorithm: ScramAlgorithm,
        client_first: &[u8],
        username: &str,
        password: &str,
    ) -> Self {
        let message = std::str::from_utf8(client_first)
            .unwrap_or_else(|error| panic!("decode SCRAM client first: {error}"));
        let client_first_bare = message
            .strip_prefix("n,,")
            .unwrap_or_else(|| panic!("SCRAM client first must use the n,, GS2 header"));
        let nonce = client_first_bare
            .strip_prefix(&format!("n={username},r="))
            .filter(|nonce| !nonce.is_empty())
            .unwrap_or_else(|| panic!("SCRAM client first must contain the prepared identity"));
        let combined_nonce = format!("{nonce}-server");
        let server_first = format!(
            "r={combined_nonce},s={},i={ITERATIONS}",
            STANDARD.encode(SALT)
        );
        let client_final_without_proof = format!("c=biws,r={combined_nonce}");
        let auth_message =
            format!("{client_first_bare},{server_first},{client_final_without_proof}");
        let mut salted_password = vec![0; algorithm.output_bytes()];
        pbkdf2::derive(
            algorithm.pbkdf2(),
            NonZeroU32::new(ITERATIONS).unwrap_or(NonZeroU32::MIN),
            SALT,
            password.as_bytes(),
            &mut salted_password,
        );
        let client_key = sign(algorithm, &salted_password, b"Client Key");
        let stored_key = digest::digest(algorithm.digest(), &client_key);
        let client_signature = sign(algorithm, stored_key.as_ref(), auth_message.as_bytes());
        let client_proof = client_key
            .iter()
            .zip(client_signature)
            .map(|(key, signature)| key ^ signature)
            .collect::<Vec<_>>();
        let client_final = format!(
            "{client_final_without_proof},p={}",
            STANDARD.encode(client_proof)
        )
        .into_bytes();
        let server_key = sign(algorithm, &salted_password, b"Server Key");
        let signature = sign(algorithm, &server_key, auth_message.as_bytes());
        let server_final = format!("v={}", STANDARD.encode(signature)).into_bytes();
        let invalid_server_final =
            format!("v={}", STANDARD.encode(vec![0; algorithm.output_bytes()])).into_bytes();
        Self {
            server_first: server_first.into_bytes(),
            client_final,
            server_final,
            invalid_server_final,
        }
    }

    pub(crate) fn server_first(&self) -> &[u8] {
        &self.server_first
    }

    pub(crate) fn client_final(&self) -> &[u8] {
        &self.client_final
    }

    pub(crate) fn server_final(&self) -> &[u8] {
        &self.server_final
    }

    pub(crate) fn invalid_server_final(&self) -> &[u8] {
        &self.invalid_server_final
    }
}

fn sign(algorithm: ScramAlgorithm, key: &[u8], message: &[u8]) -> Vec<u8> {
    let key = hmac::Key::new(algorithm.hmac(), key);
    hmac::sign(&key, message).as_ref().to_vec()
}
