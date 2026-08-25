//! Decoder-feedback proofs for reply-before-terminal TLS ordering.

use std::{mem::size_of, num::NonZeroUsize};

use bornera_core::FrameDecoder as _;
use calandria::RetainedBytes;
use kafka_driver_transport::FrameLimits;

use crate::{reactor::bornera::KafkaFrameDecoder, request::ALLOCATION_ALLOWANCE_BYTES};

use super::decoder_gate::{DecoderGate, DirectFrameDecoder};

#[test]
fn gate_stays_pending_until_every_coalesced_frame_is_offered() {
    let inner = KafkaFrameDecoder::new(limits(), nonzero(16))
        .unwrap_or_else(|error| panic!("construct gated decoder: {error}"));
    let base_limit = inner.bornera_retained_limit();
    let gate = DecoderGate::new();
    let mut decoder = DirectFrameDecoder::new(inner, Some(gate.clone()));
    let gate_bytes = u64::try_from(3 * size_of::<usize>() + ALLOCATION_ALLOWANCE_BYTES)
        .unwrap_or_else(|error| panic!("convert gate retention: {error}"));
    assert_eq!(decoder.retained_bytes(), RetainedBytes::new(gate_bytes));
    assert_eq!(
        decoder.bornera_retained_limit().get() - base_limit.get(),
        gate_bytes
    );

    let mut coalesced = framed(&[1]);
    coalesced.extend_from_slice(&framed(&[2]));
    assert!(decoder.feed(&coalesced).is_ok());
    assert!(gate.has_pending_decode());

    assert!(decoder.next_frame().is_ok_and(|frame| frame.is_some()));
    assert!(gate.has_pending_decode());
    assert!(decoder.next_frame().is_ok_and(|frame| frame.is_some()));
    assert!(gate.has_pending_decode());
    assert!(decoder.next_frame().is_ok_and(|frame| frame.is_none()));
    assert!(!gate.has_pending_decode());
    assert_eq!(decoder.retained_bytes(), RetainedBytes::new(gate_bytes));
}

fn framed(body: &[u8]) -> Vec<u8> {
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("test frame body must fit Kafka length: {error}"));
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(body);
    frame
}

fn limits() -> FrameLimits {
    FrameLimits::new(nonzero(8), nonzero(24))
        .unwrap_or_else(|error| panic!("construct gated frame limits: {error}"))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
