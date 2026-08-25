//! Exact-allocation and canonical-framing proofs for the Bornera decoder.

use std::num::NonZeroUsize;

use bornera_core::{FrameDecoder as _, FrameDriver};
use calandria::{Retained as _, RetainedBytes};
use kafka_driver_transport::{FrameDecodeError, FrameLimits};

use super::{KafkaFrameDecodeError, KafkaFrameDecoder, KafkaFrameDecoderConfigError};

#[test]
fn incremental_coalesced_frames_copy_into_exact_reply_ownership() {
    let mut decoder = decoder(16, 40, 40);
    let first = framed(&[0, 0, 0, 7, 41]);
    let second = framed(&[0, 0, 0, 8]);
    let split = 3;
    assert!(decoder.feed(&first[..split]).is_ok());
    assert!(decoder.next_frame().is_ok_and(|frame| frame.is_none()));

    let mut remainder = first[split..].to_vec();
    remainder.extend_from_slice(&second);
    assert!(decoder.feed(&remainder).is_ok());

    let Ok(Some(first)) = decoder.next_frame() else {
        panic!("first complete Kafka frame must decode");
    };
    assert_eq!(first.as_bytes(), &[0, 0, 0, 7, 41]);
    assert_eq!(first.retained_bytes().get(), 5);
    assert_eq!(first.into_bytes().as_ref(), &[0, 0, 0, 7, 41]);

    let Ok(Some(second)) = decoder.next_frame() else {
        panic!("coalesced Kafka frame must remain available");
    };
    assert_eq!(second.as_bytes(), &[0, 0, 0, 8]);
    assert_eq!(decoder.retained_bytes().get(), 0);
}

#[test]
fn decoder_reports_owned_buffer_capacity_not_visible_length() {
    let mut decoder = decoder(16, 40, 40);
    assert!(decoder.feed(&[0, 0, 0]).is_ok());
    assert!(decoder.retained_bytes().get() >= 3);
}

#[test]
fn canonical_negative_length_failure_is_terminal() {
    let mut decoder = decoder(16, 40, 40);
    assert!(decoder.feed(&(-1_i32).to_be_bytes()).is_ok());
    assert!(matches!(
        decoder.next_frame(),
        Err(KafkaFrameDecodeError::Framing(
            FrameDecodeError::NegativeFrameLength { length: -1 }
        ))
    ));
    assert!(matches!(
        decoder.next_frame(),
        Err(KafkaFrameDecodeError::DecoderFailed)
    ));
}

#[test]
fn rejected_feed_preserves_the_borrowed_input_boundary() {
    let mut decoder = decoder(4, 8, 8);
    assert!(decoder.feed(&[0; 8]).is_ok());
    assert!(decoder.feed(&[0; 8]).is_ok());
    assert!(matches!(
        decoder.feed(&[0]),
        Err(KafkaFrameDecodeError::Framing(
            FrameDecodeError::BufferCapacityExceeded {
                buffered: 16,
                incoming: 1,
                limit: 16,
            }
        ))
    ));
    assert!(decoder.next_frame().is_ok_and(|frame| frame.is_some()));
    assert!(decoder.feed(&[1]).is_ok());
}

#[test]
fn bornera_budget_admits_input_after_vec_capacity_reaches_logical_limit() {
    let mut ordinary = Vec::<u8>::new();
    ordinary.extend_from_slice(&[0, 0, 0]);
    assert_eq!(ordinary.len(), 3);
    assert!(ordinary.capacity() > ordinary.len());

    let Ok(decoder) = KafkaFrameDecoder::new(limits(4, 8), nonzero(1)) else {
        panic!("decoder budget must fit retained accounting");
    };
    assert_eq!(decoder.buffer_limits(), (8, 9));
    let retained_limit = decoder.bornera_retained_limit();
    assert_eq!(retained_limit, RetainedBytes::new(10));
    let Ok(mut driver) = FrameDriver::new(decoder, retained_limit) else {
        panic!("empty decoder must fit its Bornera limit");
    };

    assert!(driver.feed(&[0]).is_ok());
    assert!(driver.feed(&[0]).is_ok());
    assert!(driver.feed(&[0]).is_ok());
    assert_eq!(driver.retained_bytes(), RetainedBytes::new(8));
    assert!(driver.feed(&[1]).is_ok());
    assert!(driver.next_frame().is_ok_and(|frame| frame.is_none()));
    assert!(driver.feed(&[7]).is_ok());
    let Ok(Some(frame)) = driver.next_frame() else {
        panic!("fragmented frame must complete through Bornera");
    };
    assert_eq!(frame.as_bytes(), &[7]);
    assert_eq!(driver.retained_bytes(), RetainedBytes::ZERO);
}

#[test]
fn frame_driver_coalesces_a_maximum_frame_and_continues_the_next_frame() {
    let first = framed(&[1, 2, 3, 4, 5, 6, 7, 8]);
    let second = framed(&[9, 10, 11, 12, 13, 14]);
    let Ok(decoder) = KafkaFrameDecoder::new(limits(8, 12), nonzero(8)) else {
        panic!("coalescing decoder limits must fit retained accounting");
    };
    assert_eq!(decoder.buffer_limits(), (12, 20));
    let retained_limit = decoder.bornera_retained_limit();
    assert_eq!(retained_limit, RetainedBytes::new(28));
    let Ok(mut driver) = FrameDriver::new(decoder, retained_limit) else {
        panic!("empty decoder must fit its Bornera admission limit");
    };

    assert!(driver.feed(&first[..8]).is_ok());
    assert!(driver.next_frame().is_ok_and(|frame| frame.is_none()));
    assert!(driver.feed(&first[8..10]).is_ok());
    assert!(driver.next_frame().is_ok_and(|frame| frame.is_none()));

    let mut completing_chunk = first[10..].to_vec();
    completing_chunk.extend_from_slice(&second[..6]);
    assert_eq!(completing_chunk.len(), 8);
    assert!(driver.feed(&completing_chunk).is_ok());
    assert!(!driver.is_failed());
    assert!(driver.retained_bytes() <= RetainedBytes::new(20));
    let Ok(Some(decoded_first)) = driver.next_frame() else {
        panic!("maximum first frame must decode without terminal coalescing failure");
    };
    assert_eq!(decoded_first.as_bytes(), &[1, 2, 3, 4, 5, 6, 7, 8]);

    assert!(driver.feed(&second[6..]).is_ok());
    assert!(!driver.is_failed());
    let Ok(Some(decoded_second)) = driver.next_frame() else {
        panic!("retained next-frame fragment must continue decoding");
    };
    assert_eq!(decoded_second.as_bytes(), &[9, 10, 11, 12, 13, 14]);
    assert_eq!(driver.retained_bytes(), RetainedBytes::ZERO);
}

#[test]
fn declared_input_quantum_is_enforced_before_buffer_mutation() {
    let mut decoder = decoder(8, 12, 2);
    assert_eq!(
        decoder.feed(&[0, 0, 0]),
        Err(KafkaFrameDecodeError::InputChunkTooLarge {
            incoming: 3,
            limit: 2,
        })
    );
    assert_eq!(decoder.retained_bytes(), RetainedBytes::ZERO);
    assert!(decoder.feed(&[0, 0]).is_ok());
}

#[test]
fn second_input_quantum_overflow_is_rejected_during_configuration() {
    let buffered = usize::MAX - 1;
    let limits = limits(1, buffered);

    assert!(matches!(
        KafkaFrameDecoder::new(limits, nonzero(1)),
        Err(KafkaFrameDecoderConfigError::RetainedLimitOverflow {
            buffered: actual,
            max_input: 1,
        }) if actual == buffered
    ));
}

fn framed(body: &[u8]) -> Vec<u8> {
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("test frame body must fit Kafka length: {error}"));
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(body);
    frame
}

fn limits(max_frame: usize, max_buffered: usize) -> FrameLimits {
    let Some(max_frame) = NonZeroUsize::new(max_frame) else {
        panic!("test frame maximum must be nonzero");
    };
    let Some(max_buffered) = NonZeroUsize::new(max_buffered) else {
        panic!("test buffer maximum must be nonzero");
    };
    FrameLimits::new(max_frame, max_buffered)
        .unwrap_or_else(|error| panic!("test frame limits must be coherent: {error}"))
}

fn decoder(max_frame: usize, max_buffered: usize, max_input: usize) -> KafkaFrameDecoder {
    let Ok(decoder) = KafkaFrameDecoder::new(limits(max_frame, max_buffered), nonzero(max_input))
    else {
        panic!("test decoder limits must fit retained accounting");
    };
    decoder
}

fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("test value must be nonzero");
    };
    value
}
