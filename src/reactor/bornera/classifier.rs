//! Kafka response correlation classification for Bornera.

use std::fmt;

use bornera::InboundClassifier;
use bornera_core::MatchKey;
use kafka_driver_core::CorrelationId;

use super::{KafkaFrame, KafkaMatchKeyError, match_key};

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::reactor) struct KafkaReplyClassifier;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum KafkaReplyClassifierError {
    TruncatedCorrelationId { observed: usize },
    MatchKey(KafkaMatchKeyError),
}

impl fmt::Display for KafkaReplyClassifierError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TruncatedCorrelationId { observed } => write!(
                formatter,
                "Kafka response has {observed} correlation bytes; four are required"
            ),
            Self::MatchKey(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for KafkaReplyClassifierError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MatchKey(error) => Some(error),
            Self::TruncatedCorrelationId { .. } => None,
        }
    }
}

impl InboundClassifier<KafkaFrame> for KafkaReplyClassifier {
    type Error = KafkaReplyClassifierError;

    fn reply_key(&mut self, frame: &KafkaFrame) -> Result<MatchKey, Self::Error> {
        let bytes: [u8; 4] = frame
            .as_bytes()
            .get(..size_of::<i32>())
            .ok_or(KafkaReplyClassifierError::TruncatedCorrelationId {
                observed: frame.as_bytes().len(),
            })?
            .try_into()
            .map_err(|_| KafkaReplyClassifierError::TruncatedCorrelationId {
                observed: frame.as_bytes().len(),
            })?;
        match_key(CorrelationId::from_raw(i32::from_be_bytes(bytes)))
            .map_err(KafkaReplyClassifierError::MatchKey)
    }
}
