//! Public construction scenarios requiring one explicit Kafka target.

use kafka_driver::{Driver, DriverBuildError, SaslConfig};

#[test]
fn targetless_builder_is_rejected_instead_of_creating_an_inert_driver() {
    let result = Driver::builder().build_reactor();

    assert!(matches!(result, Err(DriverBuildError::MissingTarget)));
}

#[test]
fn targetless_sasl_configuration_is_rejected_instead_of_being_discarded() {
    let sasl = SaslConfig::plain("alice", "secret")
        .unwrap_or_else(|error| panic!("construct valid SASL credentials: {error}"));

    let result = Driver::builder().sasl(sasl).build_reactor();

    assert!(matches!(result, Err(DriverBuildError::MissingTarget)));
}
