//! Strict SCRAM server attribute parsing before proof work is admitted.

use std::{num::NonZeroU32, str};

use base64::{Engine as _, encoded_len, engine::general_purpose::STANDARD};
use kafka_driver_core::AuthenticationFailure;
use zeroize::Zeroizing;

use super::{limits::ScramLimits, nonce::ScramNonce};

pub(super) struct ServerFirst<'a> {
    pub(super) raw: &'a [u8],
    pub(super) nonce: &'a str,
    pub(super) salt: Zeroizing<Vec<u8>>,
    pub(super) iterations: NonZeroU32,
}

pub(super) enum ServerFinal {
    Verifier(Zeroizing<Vec<u8>>),
    Rejected,
}

pub(super) fn parse_server_first<'a>(
    response: &'a [u8],
    client_nonce: &ScramNonce,
    limits: ScramLimits,
) -> Result<ServerFirst<'a>, AuthenticationFailure> {
    let text = message_text(response)?;
    let mut nonce = None;
    let mut salt = None;
    let mut iterations = None;
    visit_attributes(text, |name, value| {
        match name {
            'm' => return Err(AuthenticationFailure::Malformed),
            'r' => nonce = Some(value),
            's' => salt = Some(value),
            'i' => iterations = Some(value),
            _ => {}
        }
        Ok(())
    })?;
    let nonce = nonce.ok_or(AuthenticationFailure::Malformed)?;
    client_nonce.validate_server(nonce, limits)?;
    let salt = decode_salt(salt.ok_or(AuthenticationFailure::Malformed)?, limits)?;
    let iterations = parse_iterations(iterations.ok_or(AuthenticationFailure::Malformed)?, limits)?;
    Ok(ServerFirst {
        raw: response,
        nonce,
        salt,
        iterations,
    })
}

pub(super) fn parse_server_final(
    response: &[u8],
    expected_signature_bytes: usize,
) -> Result<ServerFinal, AuthenticationFailure> {
    let text = message_text(response)?;
    let mut error = None;
    let mut verifier = None;
    visit_attributes(text, |name, value| {
        match name {
            'm' => return Err(AuthenticationFailure::Malformed),
            'e' => error = Some(value),
            'v' => verifier = Some(value),
            _ => {}
        }
        Ok(())
    })?;
    match (error, verifier) {
        (Some(_), None) => Ok(ServerFinal::Rejected),
        (None, Some(value)) => {
            if encoded_len(expected_signature_bytes, true).is_none_or(|max| value.len() > max) {
                return Err(AuthenticationFailure::Malformed);
            }
            let signature = STANDARD
                .decode(value)
                .map(Zeroizing::new)
                .map_err(|_| AuthenticationFailure::Malformed)?;
            if signature.len() != expected_signature_bytes {
                return Err(AuthenticationFailure::Malformed);
            }
            Ok(ServerFinal::Verifier(signature))
        }
        _ => Err(AuthenticationFailure::Malformed),
    }
}

fn message_text(message: &[u8]) -> Result<&str, AuthenticationFailure> {
    let text = str::from_utf8(message).map_err(|_| AuthenticationFailure::Malformed)?;
    if text.is_empty() || !text.is_ascii() {
        return Err(AuthenticationFailure::Malformed);
    }
    Ok(text)
}

fn visit_attributes<'a>(
    text: &'a str,
    mut visit: impl FnMut(char, &'a str) -> Result<(), AuthenticationFailure>,
) -> Result<(), AuthenticationFailure> {
    let mut seen = [false; 128];
    for attribute in text.split(',') {
        let bytes = attribute.as_bytes();
        if bytes.len() < 3 || bytes[1] != b'=' || !bytes[0].is_ascii_alphabetic() {
            return Err(AuthenticationFailure::Malformed);
        }
        let slot = usize::from(bytes[0]);
        if seen[slot] {
            return Err(AuthenticationFailure::Malformed);
        }
        seen[slot] = true;
        visit(char::from(bytes[0]), &attribute[2..])?;
    }
    Ok(())
}

fn decode_salt(
    encoded: &str,
    limits: ScramLimits,
) -> Result<Zeroizing<Vec<u8>>, AuthenticationFailure> {
    if encoded_len(limits.max_salt_bytes(), true).is_none_or(|max| encoded.len() > max) {
        return Err(AuthenticationFailure::Capacity);
    }
    let salt = STANDARD
        .decode(encoded)
        .map(Zeroizing::new)
        .map_err(|_| AuthenticationFailure::Malformed)?;
    if salt.is_empty() || salt.len() > limits.max_salt_bytes() {
        return Err(AuthenticationFailure::Malformed);
    }
    Ok(salt)
}

fn parse_iterations(value: &str, limits: ScramLimits) -> Result<NonZeroU32, AuthenticationFailure> {
    if !matches!(value.as_bytes().first(), Some(b'1'..=b'9'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(AuthenticationFailure::Malformed);
    }
    let iterations = value
        .parse::<u32>()
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(AuthenticationFailure::Malformed)?;
    if iterations.get() > limits.max_iterations() {
        return Err(AuthenticationFailure::Capacity);
    }
    Ok(iterations)
}
