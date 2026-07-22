//! Scenarios for exact qualification command admission.

use std::num::NonZeroUsize;

use super::arguments::{ArgumentError, Arguments, SaslSelection};

#[test]
fn readiness_accepts_exactly_one_bootstrap_endpoint() {
    let parsed = Arguments::parse(strings(["readiness", "127.0.0.1:9092"]));

    assert_eq!(
        parsed,
        Ok(Arguments::Readiness {
            bootstrap: "127.0.0.1:9092".to_owned(),
        })
    );
}

#[test]
fn routes_retain_the_exact_endpoint_topic_and_group() {
    let parsed = Arguments::parse(strings([
        "routes",
        "broker.test:9092",
        "orders",
        "orders-readers",
    ]));

    assert_eq!(
        parsed,
        Ok(Arguments::Routes {
            bootstrap: "broker.test:9092".to_owned(),
            topic: "orders".to_owned(),
            group: "orders-readers".to_owned(),
        })
    );
}

#[test]
fn reconnect_retains_one_exact_bootstrap_endpoint() {
    let parsed = Arguments::parse(strings(["reconnect", "broker.test:9092"]));

    assert_eq!(
        parsed,
        Ok(Arguments::Reconnect {
            bootstrap: "broker.test:9092".to_owned(),
        })
    );
}

#[test]
fn authentication_accepts_each_supported_sasl_mechanism() {
    for (name, mechanism) in [
        ("plain", SaslSelection::Plain),
        ("scram-sha-256", SaslSelection::ScramSha256),
        ("scram-sha-512", SaslSelection::ScramSha512),
    ] {
        let parsed = Arguments::parse(strings(["authenticate", name, "broker.test:9092"]));

        assert_eq!(
            parsed,
            Ok(Arguments::Authenticate {
                mechanism,
                bootstrap: "broker.test:9092".to_owned(),
            })
        );
    }
}

#[test]
fn authentication_rejects_an_unsupported_sasl_mechanism() {
    assert_eq!(
        Arguments::parse(strings(["authenticate", "oauthbearer", "one:1"])),
        Err(ArgumentError::SaslMechanism)
    );
}

#[test]
fn tls_retains_address_trust_anchor_and_server_identity() {
    let parsed = Arguments::parse(strings([
        "tls",
        "127.0.0.1:9093",
        "/tmp/test-ca.pem",
        "broker.test",
    ]));

    assert_eq!(
        parsed,
        Ok(Arguments::Tls {
            address: "127.0.0.1:9093".to_owned(),
            certificate: "/tmp/test-ca.pem".to_owned(),
            server_name: "broker.test".to_owned(),
        })
    );
}

#[test]
fn tls_authentication_retains_mechanism_and_certificate_policy() {
    let parsed = Arguments::parse(strings([
        "tls-authenticate",
        "scram-sha-512",
        "127.0.0.1:9093",
        "/tmp/test-ca.pem",
        "broker.test",
    ]));

    assert_eq!(
        parsed,
        Ok(Arguments::TlsAuthenticate {
            mechanism: SaslSelection::ScramSha512,
            address: "127.0.0.1:9093".to_owned(),
            certificate: "/tmp/test-ca.pem".to_owned(),
            server_name: "broker.test".to_owned(),
        })
    );
}

#[test]
fn partial_or_expanded_commands_are_rejected() {
    assert_eq!(
        Arguments::parse(strings(["readiness"])),
        Err(ArgumentError::Shape)
    );
    assert_eq!(
        Arguments::parse(strings(["readiness", "one:1", "two:2"])),
        Err(ArgumentError::Shape)
    );
    assert_eq!(
        Arguments::parse(strings(["routes", "one:1", "topic"])),
        Err(ArgumentError::Shape)
    );
    assert_eq!(
        Arguments::parse(strings(["reconnect", "one:1", "two:2"])),
        Err(ArgumentError::Shape)
    );
    assert_eq!(
        Arguments::parse(strings(["tls", "one:1", "ca.pem"])),
        Err(ArgumentError::Shape)
    );
}

#[test]
fn measurement_samples_are_nonzero_and_explicitly_bounded() {
    assert_eq!(
        Arguments::parse(strings(["measure", "one:1", "500"])),
        Ok(Arguments::Measure {
            bootstrap: "one:1".to_owned(),
            samples: NonZeroUsize::new(500).unwrap_or(NonZeroUsize::MIN),
        })
    );
    assert_eq!(
        Arguments::parse(strings(["measure", "one:1", "0"])),
        Err(ArgumentError::Samples)
    );
    assert_eq!(
        Arguments::parse(strings(["measure", "one:1", "10001"])),
        Err(ArgumentError::Samples)
    );
}

fn strings<const N: usize>(values: [&str; N]) -> impl Iterator<Item = String> {
    values.into_iter().map(str::to_owned)
}
