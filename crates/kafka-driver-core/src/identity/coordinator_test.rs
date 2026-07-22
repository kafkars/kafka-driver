//! Scenarios for coordinator namespaces, key bounds, and epoch exhaustion.

use super::{CoordinatorEpoch, CoordinatorKey, CoordinatorKeyError, CoordinatorKind};

#[test]
fn key_retains_its_namespace_and_exact_validated_text() {
    let key = CoordinatorKey::new(CoordinatorKind::Transaction, "orders-writer")
        .unwrap_or_else(|error| panic!("valid coordinator key rejected: {error}"));

    assert_eq!(key.kind(), CoordinatorKind::Transaction);
    assert_eq!(key.as_str(), "orders-writer");
}

#[test]
fn empty_and_oversized_keys_are_rejected_without_retaining_text() {
    assert_eq!(
        CoordinatorKey::new(CoordinatorKind::Group, ""),
        Err(CoordinatorKeyError::Empty)
    );
    let oversized = "x".repeat(CoordinatorKey::MAX_BYTES + 1);
    let error = CoordinatorKey::new(CoordinatorKind::Share, &oversized)
        .err()
        .unwrap_or_else(|| panic!("oversized key must be rejected"));

    assert!(matches!(
        error,
        CoordinatorKeyError::TooLong { bytes, limit }
            if bytes == CoordinatorKey::MAX_BYTES + 1
                && limit == CoordinatorKey::MAX_BYTES
    ));
    assert!(!format!("{error:?}").contains(&oversized));
}

#[test]
fn coordinator_epoch_exhaustion_never_wraps() {
    let last = CoordinatorEpoch::from_raw(u64::MAX);

    assert_eq!(last.get(), u64::MAX);
    assert_eq!(last.next(), None);
}
