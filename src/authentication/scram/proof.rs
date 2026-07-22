//! SCRAM client-proof derivation with zeroizing intermediate key material.

use std::num::NonZeroU32;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use ring::{digest, hmac, pbkdf2};
use zeroize::Zeroizing;

use super::algorithm::ScramAlgorithm;

pub(super) struct ScramProof {
    pub(super) client_final: Zeroizing<Vec<u8>>,
    pub(super) server_key: Zeroizing<Vec<u8>>,
    pub(super) auth_message: Zeroizing<Vec<u8>>,
}

pub(super) fn derive_proof(
    algorithm: ScramAlgorithm,
    password: &str,
    salt: &[u8],
    iterations: NonZeroU32,
    client_first_bare: &[u8],
    server_first: &[u8],
    client_final_without_proof: &[u8],
) -> ScramProof {
    let mut auth_message = Zeroizing::new(Vec::with_capacity(
        client_first_bare.len() + server_first.len() + client_final_without_proof.len() + 2,
    ));
    auth_message.extend_from_slice(client_first_bare);
    auth_message.push(b',');
    auth_message.extend_from_slice(server_first);
    auth_message.push(b',');
    auth_message.extend_from_slice(client_final_without_proof);

    let mut salted_password = Zeroizing::new(vec![0_u8; algorithm.output_len()]);
    pbkdf2::derive(
        algorithm.pbkdf2(),
        iterations,
        salt,
        password.as_bytes(),
        &mut salted_password,
    );
    let client_key = sign(algorithm, &salted_password, b"Client Key");
    let stored_key = Zeroizing::new(
        digest::digest(algorithm.digest(), &client_key)
            .as_ref()
            .to_vec(),
    );
    let client_signature = sign(algorithm, &stored_key, &auth_message);
    let client_proof = Zeroizing::new(
        client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(key, signature)| key ^ signature)
            .collect::<Vec<_>>(),
    );
    let encoded_proof = Zeroizing::new(STANDARD.encode(&client_proof));
    let mut client_final = Zeroizing::new(Vec::with_capacity(
        client_final_without_proof.len() + encoded_proof.len() + 3,
    ));
    client_final.extend_from_slice(client_final_without_proof);
    client_final.extend_from_slice(b",p=");
    client_final.extend_from_slice(encoded_proof.as_bytes());

    ScramProof {
        client_final,
        server_key: sign(algorithm, &salted_password, b"Server Key"),
        auth_message,
    }
}

fn sign(algorithm: ScramAlgorithm, key: &[u8], message: &[u8]) -> Zeroizing<Vec<u8>> {
    let key = hmac::Key::new(algorithm.hmac(), key);
    Zeroizing::new(hmac::sign(&key, message).as_ref().to_vec())
}
