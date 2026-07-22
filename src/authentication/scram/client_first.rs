//! Bounded SCRAM client-first construction and username escaping.

use kafka_driver_core::AuthenticationFailure;
use zeroize::Zeroizing;

use super::nonce::ScramNonce;

pub(super) struct ClientFirst {
    pub(super) message: Zeroizing<Vec<u8>>,
    pub(super) bare: Zeroizing<Vec<u8>>,
}

pub(super) fn client_first(
    username: &str,
    nonce: &ScramNonce,
    max_bytes: usize,
) -> Result<ClientFirst, AuthenticationFailure> {
    let mut bare = Zeroizing::new(Vec::with_capacity(username.len().min(max_bytes)));
    bare.extend_from_slice(b"n=");
    for byte in username.bytes() {
        let escaped = match byte {
            b',' => b"=2C".as_slice(),
            b'=' => b"=3D".as_slice(),
            _ => std::slice::from_ref(&byte),
        };
        if bare
            .len()
            .saturating_add(escaped.len() + nonce.as_str().len() + 3)
            > max_bytes
        {
            return Err(AuthenticationFailure::Capacity);
        }
        bare.extend_from_slice(escaped);
    }
    bare.extend_from_slice(b",r=");
    bare.extend_from_slice(nonce.as_str().as_bytes());
    if bare.len().saturating_add(3) > max_bytes {
        return Err(AuthenticationFailure::Capacity);
    }
    let mut message = Zeroizing::new(Vec::with_capacity(bare.len() + 3));
    message.extend_from_slice(b"n,,");
    message.extend_from_slice(&bare);
    Ok(ClientFirst { message, bare })
}
