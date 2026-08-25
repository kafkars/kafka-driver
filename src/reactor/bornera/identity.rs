//! Checked conversion between Kafka correlation IDs and Bornera match keys.

use std::fmt;

use bornera_core::MatchKey;
use kafka_driver_core::CorrelationId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum KafkaMatchKeyError {
    NegativeCorrelationId(i32),
    MatchKeyOutOfRange(u32),
}

impl fmt::Display for KafkaMatchKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativeCorrelationId(correlation) => {
                write!(formatter, "negative Kafka correlation ID {correlation}")
            }
            Self::MatchKeyOutOfRange(key) => {
                write!(
                    formatter,
                    "Bornera match key {key} exceeds Kafka's signed domain"
                )
            }
        }
    }
}

impl std::error::Error for KafkaMatchKeyError {}

pub(in crate::reactor) fn match_key(
    correlation: CorrelationId,
) -> Result<MatchKey, KafkaMatchKeyError> {
    let raw = correlation.get();
    let key = u32::try_from(raw).map_err(|_| KafkaMatchKeyError::NegativeCorrelationId(raw))?;
    Ok(MatchKey::new(key))
}

pub(in crate::reactor) fn correlation_id(
    key: MatchKey,
) -> Result<CorrelationId, KafkaMatchKeyError> {
    let raw = key.get();
    let signed = i32::try_from(raw).map_err(|_| KafkaMatchKeyError::MatchKeyOutOfRange(raw))?;
    Ok(CorrelationId::from_raw(signed))
}
