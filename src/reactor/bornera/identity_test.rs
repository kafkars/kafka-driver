//! Checked Kafka and Bornera identity conversion proofs.

use bornera_core::MatchKey;
use kafka_driver_core::CorrelationId;

use super::{KafkaMatchKeyError, correlation_id, match_key};

#[test]
fn nonnegative_kafka_domain_round_trips() {
    for raw in [0, 1, i32::MAX] {
        let Ok(key) = match_key(CorrelationId::from_raw(raw)) else {
            panic!("nonnegative Kafka correlation must become a match key");
        };
        let Ok(expected) = u32::try_from(raw) else {
            panic!("nonnegative Kafka correlation must fit a match key");
        };
        assert_eq!(key.get(), expected);
        assert_eq!(correlation_id(key), Ok(CorrelationId::from_raw(raw)));
    }
}

#[test]
fn conversions_reject_values_outside_kafkas_nonnegative_domain() {
    const FIRST_OUTSIDE_SIGNED_DOMAIN: u32 = 2_147_483_648;

    assert_eq!(
        match_key(CorrelationId::from_raw(-1)),
        Err(KafkaMatchKeyError::NegativeCorrelationId(-1))
    );
    assert_eq!(
        correlation_id(MatchKey::new(FIRST_OUTSIDE_SIGNED_DOMAIN)),
        Err(KafkaMatchKeyError::MatchKeyOutOfRange(
            FIRST_OUTSIDE_SIGNED_DOMAIN
        ))
    );
}
