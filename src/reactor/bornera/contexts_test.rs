//! Affine semantic-context reservation and publication proofs.

use std::num::NonZeroUsize;

use bornera_core::{ConnectionEpoch, OperationId};
use calandria::RetainedBytes;

use super::{ContextPublishFailure, ContextReserveFailure, OperationContextKey, OperationContexts};

#[test]
fn dropping_an_unpublished_reservation_rolls_back_both_bounds() {
    let contexts = contexts(1, 8);
    let Ok(reservation) = contexts.reserve("first", RetainedBytes::new(8)) else {
        panic!("first context must reserve");
    };
    assert_eq!(contexts.snapshot().reserved(), 1);
    assert_eq!(contexts.snapshot().retained_bytes(), RetainedBytes::new(8));
    assert!(!contexts.snapshot().is_poisoned());

    drop(reservation);

    assert_eq!(contexts.snapshot().reserved(), 0);
    assert_eq!(contexts.snapshot().retained_bytes(), RetainedBytes::ZERO);
}

#[test]
fn unpublished_reservations_participate_in_count_capacity() {
    let contexts = contexts(1, 16);
    let Ok(first) = contexts.reserve("first", RetainedBytes::new(4)) else {
        panic!("first context must reserve");
    };
    let Err(rejected) = contexts.reserve("second", RetainedBytes::new(4)) else {
        panic!("unpublished reservation must consume the only slot");
    };
    assert_eq!(
        rejected.failure(),
        ContextReserveFailure::CapacityReached { limit: 1 }
    );
    assert_eq!(rejected.into_context(), "second");
    drop(first);
}

#[test]
fn publication_keys_the_exact_context_until_terminal_release() {
    let contexts = contexts(2, 16);
    let Ok(reservation) = contexts.reserve("owned", RetainedBytes::new(5)) else {
        panic!("context must reserve");
    };
    assert!(reservation.publish(key(1, 7)).is_ok());
    let snapshot = contexts.snapshot();
    assert_eq!(snapshot.reserved(), 0);
    assert_eq!(snapshot.published(), 1);
    assert_eq!(snapshot.retained_bytes(), RetainedBytes::new(5));

    assert_eq!(contexts.release(key(1, 7)), Some("owned"));
    assert_eq!(contexts.snapshot().published(), 0);
    assert_eq!(contexts.snapshot().retained_bytes(), RetainedBytes::ZERO);
}

#[test]
fn duplicate_publication_preserves_the_rejected_context_and_accounting() {
    let contexts = contexts(2, 16);
    let Ok(first) = contexts.reserve("first", RetainedBytes::new(5)) else {
        panic!("first context must reserve");
    };
    assert!(first.publish(key(1, 9)).is_ok());
    let Ok(second) = contexts.reserve("second", RetainedBytes::new(6)) else {
        panic!("second context must reserve");
    };
    let Err(error) = second.publish(key(1, 9)) else {
        panic!("duplicate operation ID must reject publication");
    };
    assert_eq!(
        error.failure(),
        ContextPublishFailure::OperationInUse { key: key(1, 9) }
    );
    assert_eq!(error.into_context(), "second");
    assert_eq!(contexts.snapshot().published(), 1);
    assert_eq!(contexts.snapshot().retained_bytes(), RetainedBytes::new(5));
}

#[test]
fn retained_byte_rejection_returns_the_unpublished_context() {
    let contexts = contexts(2, 7);
    let Err(error) = contexts.reserve("too large", RetainedBytes::new(8)) else {
        panic!("context above the byte limit must reject");
    };
    assert_eq!(
        error.failure(),
        ContextReserveFailure::RetainedByteCapacity {
            limit: RetainedBytes::new(7),
        }
    );
    assert_eq!(error.into_context(), "too large");
}

#[test]
fn publication_after_owner_drop_returns_the_reserved_context() {
    let contexts = contexts(1, 8);
    let Ok(reservation) = contexts.reserve("reserved", RetainedBytes::new(8)) else {
        panic!("context must reserve");
    };
    drop(contexts);
    let Err(error) = reservation.publish(key(1, 1)) else {
        panic!("a dropped owner cannot accept publication");
    };
    assert_eq!(error.failure(), ContextPublishFailure::OwnerDropped);
    assert_eq!(error.into_context(), "reserved");
}

#[test]
fn abort_returns_the_bound_context_and_rolls_back_both_bounds() {
    #[derive(Debug, Eq, PartialEq)]
    struct Context {
        name: &'static str,
        correlation: Option<i32>,
    }

    let contexts = OperationContexts::new(NonZeroUsize::MIN, RetainedBytes::new(8));
    let Ok(mut reservation) = contexts.reserve(
        Context {
            name: "owned",
            correlation: None,
        },
        RetainedBytes::new(8),
    ) else {
        panic!("context must reserve");
    };
    reservation.bind(|context| context.correlation = Some(17));
    assert_eq!(contexts.snapshot().retained_bytes(), RetainedBytes::new(8));

    assert_eq!(
        reservation.abort(),
        Context {
            name: "owned",
            correlation: Some(17),
        }
    );
    assert_eq!(contexts.snapshot().reserved(), 0);
    assert_eq!(contexts.snapshot().retained_bytes(), RetainedBytes::ZERO);
}

#[test]
fn stale_epoch_release_cannot_take_a_reused_operation_id() {
    let contexts = contexts(2, 16);
    let Ok(old) = contexts.reserve("old", RetainedBytes::new(4)) else {
        panic!("old context must reserve");
    };
    let Ok(current) = contexts.reserve("current", RetainedBytes::new(5)) else {
        panic!("current context must reserve");
    };
    assert!(old.publish(key(4, 0)).is_ok());
    assert!(current.publish(key(5, 0)).is_ok());

    assert_eq!(contexts.release(key(3, 0)), None);
    assert_eq!(contexts.release(key(4, 0)), Some("old"));
    assert_eq!(contexts.release(key(5, 0)), Some("current"));
    assert_eq!(contexts.snapshot().retained_bytes(), RetainedBytes::ZERO);
}

#[test]
fn release_next_and_drain_are_key_ordered_and_batch_bounded() {
    let contexts = contexts(4, 32);
    for (key, name) in [
        (key(2, 1), "epoch-two"),
        (key(1, 9), "operation-nine"),
        (key(1, 3), "operation-three"),
    ] {
        let Ok(reservation) = contexts.reserve(name, RetainedBytes::new(2)) else {
            panic!("context must reserve");
        };
        assert!(reservation.publish(key).is_ok());
    }

    assert_eq!(
        contexts.release_next(),
        Some((key(1, 3), "operation-three"))
    );
    assert_eq!(
        contexts.drain(NonZeroUsize::MIN),
        vec![(key(1, 9), "operation-nine")]
    );
    assert_eq!(contexts.snapshot().published(), 1);
    assert_eq!(contexts.snapshot().retained_bytes(), RetainedBytes::new(2));
    let Some(remaining_limit) = NonZeroUsize::new(4) else {
        panic!("test drain limit must be nonzero");
    };
    assert_eq!(
        contexts.drain(remaining_limit),
        vec![(key(2, 1), "epoch-two")]
    );
    assert_eq!(contexts.snapshot().retained_bytes(), RetainedBytes::ZERO);
}

fn contexts(max_contexts: usize, max_retained_bytes: u64) -> OperationContexts<&'static str> {
    let Some(max_contexts) = NonZeroUsize::new(max_contexts) else {
        panic!("test context capacity must be nonzero");
    };
    OperationContexts::new(max_contexts, RetainedBytes::new(max_retained_bytes))
}

const fn key(epoch: u64, operation: u64) -> OperationContextKey {
    OperationContextKey::new(ConnectionEpoch::new(epoch), OperationId::new(operation))
}
