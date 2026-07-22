//! Boundary scenarios for capped exponential reconnect jitter.

use std::time::Duration;

use super::{BackoffPolicy, BackoffPolicyError, JitterSample, RetryOrdinal};

#[test]
fn invalid_bounds_are_rejected_at_construction() {
    assert_eq!(
        BackoffPolicy::try_new(Duration::ZERO, Duration::from_secs(1)),
        Err(BackoffPolicyError::ZeroBase)
    );
    assert_eq!(
        BackoffPolicy::try_new(Duration::from_secs(2), Duration::from_secs(1)),
        Err(BackoffPolicyError::MaxBelowBase)
    );
    assert_eq!(
        BackoffPolicy::try_new(Duration::from_secs(u64::MAX), Duration::from_secs(u64::MAX)),
        Err(BackoffPolicyError::DurationTooLarge)
    );
}

#[test]
fn first_retry_uses_equal_jitter_with_a_nonzero_floor() {
    let policy = policy(100, 1_000);
    let retry = ordinal(1);

    assert_eq!(
        policy.delay(retry, JitterSample::from_raw(0)),
        Duration::from_nanos(50)
    );
    assert_eq!(
        policy.delay(retry, JitterSample::from_raw(50)),
        Duration::from_nanos(100)
    );
}

#[test]
fn exponential_cap_stays_bounded_for_large_ordinals() {
    let policy = policy(100, 250);

    assert_eq!(
        policy.delay(ordinal(2), JitterSample::from_raw(200)),
        Duration::from_nanos(125 + 74)
    );
    assert_eq!(
        policy.delay(ordinal(u32::MAX), JitterSample::from_raw(125)),
        Duration::from_nanos(250)
    );
}

fn policy(base_nanos: u64, max_nanos: u64) -> BackoffPolicy {
    BackoffPolicy::try_new(
        Duration::from_nanos(base_nanos),
        Duration::from_nanos(max_nanos),
    )
    .unwrap_or_else(|error| panic!("test backoff policy must be valid: {error}"))
}

fn ordinal(value: u32) -> RetryOrdinal {
    RetryOrdinal::from_raw(value)
        .unwrap_or_else(|| panic!("test retry ordinal must be nonzero: {value}"))
}
