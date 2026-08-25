//! Kafka correlation-domain proofs for Bornera reply classification.

use bornera::InboundClassifier as _;

use super::{KafkaFrame, KafkaMatchKeyError, KafkaReplyClassifier, KafkaReplyClassifierError};

#[test]
fn classifier_reads_the_signed_correlation_prefix() {
    let frame = KafkaFrame::copy_from_slice(&[0x7f, 0xff, 0xff, 0xff, 9, 8]);
    let Ok(key) = KafkaReplyClassifier.reply_key(&frame) else {
        panic!("largest nonnegative Kafka correlation must classify");
    };
    assert_eq!(key.get(), i32::MAX as u32);
}

#[test]
fn classifier_rejects_a_truncated_header() {
    let frame = KafkaFrame::copy_from_slice(&[0, 0, 1]);
    assert_eq!(
        KafkaReplyClassifier.reply_key(&frame),
        Err(KafkaReplyClassifierError::TruncatedCorrelationId { observed: 3 })
    );
}

#[test]
fn classifier_rejects_the_negative_half_of_kafkas_signed_domain() {
    let frame = KafkaFrame::copy_from_slice(&(-1_i32).to_be_bytes());
    assert_eq!(
        KafkaReplyClassifier.reply_key(&frame),
        Err(KafkaReplyClassifierError::MatchKey(
            KafkaMatchKeyError::NegativeCorrelationId(-1)
        ))
    );
}
