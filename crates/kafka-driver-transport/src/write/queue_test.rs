//! Scenarios for bounded admission, FIFO slicing, and exact write progress.

use std::num::NonZeroUsize;

use bytes::Bytes;
use kafka_driver_core::{CallId, Delivery, EffectId};

use super::{
    WriteAdmissionFailure, WriteIdentityKind, WriteProgress, WriteProgressError, WriteQueue,
    WriteQueueLimits,
};

#[test]
fn admission_transfers_delivery_boundary_and_front_is_chunked() {
    let mut queue = queue(2, 16);
    let frame = framed(&[10, 20, 30]);

    let Ok(accepted) = queue.admit(call(1), effect(11), frame.clone()) else {
        panic!("bounded frame must be admitted");
    };
    let Some(front) = queue.front(nonzero(5)) else {
        panic!("admitted frame must be writable");
    };

    assert_eq!(accepted.call_id(), call(1));
    assert_eq!(accepted.effect_id(), effect(11));
    assert_eq!(accepted.frame_bytes(), frame.len());
    assert_eq!(accepted.delivery(), Delivery::PossiblySent);
    assert_eq!(front.call_id(), call(1));
    assert_eq!(front.effect_id(), effect(11));
    assert_eq!(front.bytes(), &frame[..5]);
    assert_eq!(queue.queued_frames(), 1);
    assert_eq!(queue.buffered_bytes(), frame.len());
}

#[test]
fn partial_progress_preserves_fifo_until_the_front_completes() {
    let mut queue = queue(2, 32);
    let first = framed(&[1, 2, 3]);
    let second = framed(&[4, 5]);
    admit(&mut queue, 1, 11, first.clone());
    admit(&mut queue, 2, 12, second.clone());

    assert_eq!(
        queue.advance(effect(11), 3),
        Ok(WriteProgress::Pending {
            call_id: call(1),
            effect_id: effect(11),
            remaining: first.len() - 3,
        })
    );
    let Some(front) = queue.front(nonzero(16)) else {
        panic!("partially written frame must remain at the front");
    };
    assert_eq!(front.bytes(), &first[3..]);

    assert_eq!(
        queue.advance(effect(11), first.len() - 3),
        Ok(WriteProgress::Complete {
            call_id: call(1),
            effect_id: effect(11),
            frame_bytes: first.len(),
        })
    );
    let Some(front) = queue.front(nonzero(16)) else {
        panic!("second frame must become writable");
    };
    assert_eq!(front.call_id(), call(2));
    assert_eq!(front.bytes(), second.as_ref());
    assert_eq!(queue.buffered_bytes(), second.len());
}

#[test]
fn invalid_progress_does_not_mutate_the_fifo_front() {
    let mut queue = queue(2, 16);
    let frame = framed(&[1, 2]);
    admit(&mut queue, 1, 11, frame.clone());

    assert_eq!(
        queue.advance(effect(12), 1),
        Err(WriteProgressError::OutOfOrderEffect {
            expected: effect(11),
            received: effect(12),
        })
    );
    assert_eq!(
        queue.advance(effect(11), frame.len() + 1),
        Err(WriteProgressError::ExceedsRemaining {
            written: frame.len() + 1,
            remaining: frame.len(),
        })
    );
    let Some(front) = queue.front(nonzero(16)) else {
        panic!("invalid progress must retain the queue front");
    };
    assert_eq!(front.bytes(), frame.as_ref());
}

#[test]
fn count_capacity_returns_the_exact_unsent_frame() {
    let mut queue = queue(1, 32);
    admit(&mut queue, 1, 11, framed(&[1]));
    let rejected = framed(&[2, 3]);

    let Err(error) = queue.admit(call(2), effect(12), rejected.clone()) else {
        panic!("second frame must exceed count capacity");
    };

    assert_eq!(
        error.failure(),
        WriteAdmissionFailure::FrameCapacityReached { limit: 1 }
    );
    assert_eq!(error.delivery(), Delivery::NotSent);
    assert_eq!(error.into_frame(), rejected);
}

#[test]
fn byte_capacity_returns_the_exact_unsent_frame() {
    let first = framed(&[1]);
    let rejected = framed(&[2, 3]);
    let mut queue = queue(2, first.len() + rejected.len() - 1);
    admit(&mut queue, 1, 11, first.clone());

    let Err(error) = queue.admit(call(2), effect(12), rejected.clone()) else {
        panic!("second frame must exceed byte capacity");
    };

    assert_eq!(
        error.failure(),
        WriteAdmissionFailure::ByteCapacityReached {
            buffered: first.len(),
            incoming: rejected.len(),
            limit: first.len() + rejected.len() - 1,
        }
    );
    assert_eq!(error.delivery(), Delivery::NotSent);
    assert_eq!(error.into_frame(), rejected);
}

#[test]
fn pending_call_and_effect_identities_cannot_be_reused() {
    let mut queue = queue(3, 32);
    admit(&mut queue, 1, 11, framed(&[1]));

    let Err(call_error) = queue.admit(call(1), effect(12), framed(&[2])) else {
        panic!("pending call identity must remain unique");
    };
    let Err(effect_error) = queue.admit(call(2), effect(11), framed(&[3])) else {
        panic!("pending effect identity must remain unique");
    };

    assert_eq!(
        call_error.failure(),
        WriteAdmissionFailure::IdentityInUse(WriteIdentityKind::Call)
    );
    assert_eq!(
        effect_error.failure(),
        WriteAdmissionFailure::IdentityInUse(WriteIdentityKind::Effect)
    );
    assert_eq!(queue.queued_frames(), 1);
}

#[test]
fn short_frame_is_rejected_before_it_can_claim_an_identity() {
    let mut queue = queue(1, 8);
    let short = Bytes::from_static(&[0, 0, 0]);

    let Err(error) = queue.admit(call(1), effect(11), short.clone()) else {
        panic!("a frame without a complete length prefix must be rejected");
    };

    assert_eq!(
        error.failure(),
        WriteAdmissionFailure::FrameTooShort {
            bytes: 3,
            minimum: 4,
        }
    );
    assert_eq!(error.into_frame(), short);
    assert_eq!(queue.queued_frames(), 0);
}

#[test]
fn discard_releases_original_bytes_even_after_partial_progress() {
    let mut queue = queue(2, 32);
    let first = framed(&[1, 2]);
    let second = framed(&[3]);
    admit(&mut queue, 1, 11, first.clone());
    admit(&mut queue, 2, 12, second.clone());
    assert!(matches!(
        queue.advance(effect(11), 2),
        Ok(WriteProgress::Pending { .. })
    ));

    assert_eq!(
        queue.discard_all(),
        super::DiscardedWrites {
            frames: 2,
            bytes: first.len() + second.len(),
        }
    );
    assert_eq!(queue.queued_frames(), 0);
    assert_eq!(queue.buffered_bytes(), 0);
    assert_eq!(queue.front(nonzero(1)), None);
    assert_eq!(
        queue.advance(effect(11), 1),
        Err(WriteProgressError::NoPendingWrite)
    );
}

#[test]
fn diagnostics_report_capacity_without_exposing_queued_frame_bytes() {
    let mut queue = queue(2, 64);
    admit(&mut queue, 1, 11, framed(b"private-credential"));

    let diagnostic = format!("{queue:?}");

    assert!(diagnostic.contains("queued_frames: 1"));
    assert!(!diagnostic.contains("private-credential"));
}

fn queue(max_frames: usize, max_bytes: usize) -> WriteQueue {
    WriteQueue::new(WriteQueueLimits::new(
        nonzero(max_frames),
        nonzero(max_bytes),
    ))
}

fn admit(queue: &mut WriteQueue, call_id: u64, effect_id: u64, frame: Bytes) {
    if queue
        .admit(call(call_id), effect(effect_id), frame)
        .is_err()
    {
        panic!("test frame must be admitted");
    }
}

fn framed(body: &[u8]) -> Bytes {
    let Ok(length) = i32::try_from(body.len()) else {
        panic!("test body length must fit the Kafka prefix");
    };
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(body);
    Bytes::from(frame)
}

const fn call(value: u64) -> CallId {
    CallId::from_raw(value)
}

const fn effect(value: u64) -> EffectId {
    EffectId::from_raw(value)
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("test limit must be nonzero");
    };
    value
}
