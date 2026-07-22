//! Exact wire-name checks for generated SASL handshake selection.

use super::SaslMechanism;

#[test]
fn every_mechanism_exposes_its_standard_kafka_name() {
    assert_eq!(SaslMechanism::Plain.name(), "PLAIN");
    assert_eq!(SaslMechanism::ScramSha256.name(), "SCRAM-SHA-256");
    assert_eq!(SaslMechanism::ScramSha512.name(), "SCRAM-SHA-512");
}
