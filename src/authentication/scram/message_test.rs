//! Given/When/Then checks for bounded, strict SCRAM server attributes.

use kafka_driver_core::AuthenticationFailure;

use super::{
    limits::ScramLimits,
    message::{ServerFinal, parse_server_final, parse_server_first},
    nonce::ScramNonce,
};

#[test]
fn challenge_accepts_exact_cpu_and_salt_limits() {
    let limits = ScramLimits::new(32, 3, 4_096);
    let nonce = ScramNonce::new("client", limits)
        .unwrap_or_else(|failure| panic!("valid nonce: {failure:?}"));

    let challenge = parse_server_first(b"r=clientserver,s=YWJj,i=4096", &nonce, limits)
        .unwrap_or_else(|failure| panic!("bounded challenge: {failure:?}"));

    assert_eq!(challenge.nonce, "clientserver");
    assert_eq!(challenge.salt.as_slice(), b"abc");
    assert_eq!(challenge.iterations.get(), 4_096);
}

#[test]
fn challenge_rejects_work_one_iteration_above_the_cpu_limit() {
    let limits = ScramLimits::new(32, 3, 4_096);
    let nonce = ScramNonce::new("client", limits)
        .unwrap_or_else(|failure| panic!("valid nonce: {failure:?}"));

    assert_eq!(
        parse_server_first(b"r=clientserver,s=YWJj,i=4097", &nonce, limits,).err(),
        Some(AuthenticationFailure::Capacity)
    );
}

#[test]
fn challenge_rejects_oversized_salt_before_decoding() {
    let limits = ScramLimits::new(32, 3, 4_096);
    let nonce = ScramNonce::new("client", limits)
        .unwrap_or_else(|failure| panic!("valid nonce: {failure:?}"));

    assert_eq!(
        parse_server_first(b"r=clientserver,s=YWJjZA==,i=4096", &nonce, limits,).err(),
        Some(AuthenticationFailure::Capacity)
    );
}

#[test]
fn challenge_rejects_nonce_substitution_and_duplicate_attributes() {
    let limits = ScramLimits::default();
    let nonce = ScramNonce::new("client", limits)
        .unwrap_or_else(|failure| panic!("valid nonce: {failure:?}"));

    for malformed in [
        b"r=server,s=YWJj,i=4096".as_slice(),
        b"r=clientserver,r=again,s=YWJj,i=4096".as_slice(),
        b"m=required,r=clientserver,s=YWJj,i=4096".as_slice(),
    ] {
        assert_eq!(
            parse_server_first(malformed, &nonce, limits).err(),
            Some(AuthenticationFailure::Malformed)
        );
    }
}

#[test]
fn server_final_distinguishes_rejection_from_malformed_verifiers() {
    assert!(matches!(
        parse_server_final(b"e=invalid-proof", 32),
        Ok(ServerFinal::Rejected)
    ));
    assert_eq!(
        parse_server_final(b"v=not-base64", 32).err(),
        Some(AuthenticationFailure::Malformed)
    );
    assert_eq!(
        parse_server_final(b"e=failed,v=YWJj", 3).err(),
        Some(AuthenticationFailure::Malformed)
    );
}
