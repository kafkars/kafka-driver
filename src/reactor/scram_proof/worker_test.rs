//! Focused real-worker scenario for proof completion identity and reactor wake.

use std::{num::NonZeroUsize, time::Duration};

use crate::{
    ScramProofLimits,
    reactor::{Poller, WakeHandle},
};

use super::{
    ScramProofWorker,
    queue_test::{assert_continues, request},
};

#[test]
fn completed_proof_wakes_the_reactor_with_exact_request_identity() {
    let mut poller = Poller::new(NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("create test poller: {error}"));
    let wake = WakeHandle::new(poller.wake_handle());
    let worker = ScramProofWorker::spawn(ScramProofLimits::default(), wake)
        .unwrap_or_else(|error| panic!("spawn proof worker: {error}"));
    let expected = request(7);
    let expected_debug = format!("{expected:?}");

    worker
        .sender()
        .submit(expected)
        .unwrap_or_else(|error| panic!("admit proof: {error}"));
    let mut events = Vec::new();
    poller
        .poll_into(Some(Duration::from_secs(1)), &mut events)
        .unwrap_or_else(|error| panic!("wait for proof worker wake: {error}"));
    let mut outcomes = Vec::new();
    let progress = worker
        .drain_into(&mut outcomes)
        .unwrap_or_else(|error| panic!("drain proof outcome: {error}"));

    assert_eq!(progress.outcomes(), 1);
    assert!(!progress.more_work());
    let outcome = outcomes
        .pop()
        .unwrap_or_else(|| panic!("proof outcome missing"));
    assert!(format!("{outcome:?}").contains("effect_id: EffectId(7)"));
    assert!(expected_debug.contains("effect_id: EffectId(7)"));
    assert_continues(outcome);
    worker
        .shutdown()
        .unwrap_or_else(|error| panic!("join proof worker: {error}"));
}

#[test]
fn shutdown_joins_a_worker_blocked_by_full_outcome_capacity() {
    let mut poller = Poller::new(NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("create test poller: {error}"));
    let wake = WakeHandle::new(poller.wake_handle());
    let limits = ScramProofLimits::new(nonzero(2), NonZeroUsize::MIN, NonZeroUsize::MIN);
    let worker = ScramProofWorker::spawn(limits, wake)
        .unwrap_or_else(|error| panic!("spawn proof worker: {error}"));
    let sender = worker.sender();
    for raw in 1..=2 {
        sender
            .submit(request(raw))
            .unwrap_or_else(|error| panic!("admit proof request: {error}"));
    }
    drop(sender);
    let mut events = Vec::new();
    poller
        .poll_into(Some(Duration::from_secs(1)), &mut events)
        .unwrap_or_else(|error| panic!("wait for first proof outcome: {error}"));

    worker
        .shutdown()
        .unwrap_or_else(|error| panic!("join capacity-blocked proof worker: {error}"));
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
