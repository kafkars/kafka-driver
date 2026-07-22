//! Scenarios for fragmented, coalesced, oversized, and malformed Kafka frames.

use std::num::NonZeroUsize;

use super::{FrameDecodeError, FrameDecoder, FrameLimits, FrameLimitsError};

#[test]
fn fragmented_prefix_and_body_emit_only_when_complete() {
    let mut decoder = decoder(8, 12);
    let frame = framed(b"hello");

    assert_eq!(decoder.feed(&frame[..2]), Ok(()));
    assert_eq!(decoder.next_frame(), Ok(None));
    assert_eq!(decoder.feed(&frame[2..7]), Ok(()));
    assert_eq!(decoder.next_frame(), Ok(None));
    assert_eq!(decoder.feed(&frame[7..]), Ok(()));

    let Ok(Some(body)) = decoder.next_frame() else {
        panic!("a complete fragmented frame must be emitted");
    };
    assert_eq!(body.as_bytes(), b"hello");
    assert_eq!(body.len(), 5);
    assert!(!body.is_empty());
    assert_eq!(decoder.buffered_bytes(), 0);
}

#[test]
fn coalesced_frames_remain_fifo_and_zero_length_is_valid_framing() {
    let mut decoder = decoder(8, 20);
    let mut input = framed(b"first");
    input.extend_from_slice(&framed(b""));

    assert_eq!(decoder.feed(&input), Ok(()));
    let Ok(Some(first)) = decoder.next_frame() else {
        panic!("the first coalesced frame must be available");
    };
    let Ok(Some(second)) = decoder.next_frame() else {
        panic!("the second coalesced frame must be available");
    };

    assert_eq!(first.into_bytes().as_ref(), b"first");
    assert!(second.is_empty());
    assert_eq!(decoder.next_frame(), Ok(None));
}

#[test]
fn negative_length_is_terminal_for_stream_alignment() {
    let mut decoder = decoder(8, 12);
    assert_eq!(decoder.feed(&(-1_i32).to_be_bytes()), Ok(()));

    assert_eq!(
        decoder.next_frame(),
        Err(FrameDecodeError::NegativeFrameLength { length: -1 })
    );
    assert_eq!(decoder.next_frame(), Err(FrameDecodeError::DecoderFailed));
    assert_eq!(decoder.feed(&[]), Err(FrameDecodeError::DecoderFailed));
}

#[test]
fn declared_oversize_is_rejected_before_body_arrives() {
    let mut decoder = decoder(4, 8);
    assert_eq!(decoder.feed(&5_i32.to_be_bytes()), Ok(()));

    assert_eq!(
        decoder.next_frame(),
        Err(FrameDecodeError::FrameTooLarge {
            length: 5,
            limit: 4,
        })
    );
}

#[test]
fn aggregate_buffer_rejection_does_not_consume_the_chunk_or_poison() {
    let mut decoder = decoder(4, 8);
    assert_eq!(decoder.feed(&[0, 0, 0]), Ok(()));

    assert_eq!(
        decoder.feed(&[0; 6]),
        Err(FrameDecodeError::BufferCapacityExceeded {
            buffered: 3,
            incoming: 6,
            limit: 8,
        })
    );
    assert_eq!(decoder.buffered_bytes(), 3);
    assert_eq!(decoder.feed(&[0]), Ok(()));
    assert!(matches!(decoder.next_frame(), Ok(Some(_))));
}

#[test]
fn limits_require_space_for_one_maximum_frame() {
    let Some(frame) = NonZeroUsize::new(8) else {
        panic!("test frame limit is nonzero");
    };
    let Some(buffer) = NonZeroUsize::new(11) else {
        panic!("test buffer limit is nonzero");
    };

    assert_eq!(
        FrameLimits::new(frame, buffer),
        Err(FrameLimitsError::BufferCannotHoldMaximumFrame {
            required: 12,
            configured: 11,
        })
    );
}

fn decoder(max_frame: usize, max_buffered: usize) -> FrameDecoder {
    let Some(max_frame) = NonZeroUsize::new(max_frame) else {
        panic!("test frame limit is nonzero");
    };
    let Some(max_buffered) = NonZeroUsize::new(max_buffered) else {
        panic!("test buffer limit is nonzero");
    };
    let Ok(limits) = FrameLimits::new(max_frame, max_buffered) else {
        panic!("test limits must hold one complete frame");
    };
    FrameDecoder::new(limits)
}

fn framed(body: &[u8]) -> Vec<u8> {
    let Ok(length) = i32::try_from(body.len()) else {
        panic!("test body length fits the Kafka prefix");
    };
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(body);
    frame
}
