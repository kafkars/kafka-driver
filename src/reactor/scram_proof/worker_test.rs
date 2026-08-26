//! Focused worker scenarios for proof identity, wake, and nonblocking teardown.

use std::{
    num::NonZeroUsize,
    sync::mpsc::{Receiver, Sender, channel},
    thread,
    time::Duration,
};

use crate::{ScramProofLimits, reactor::wake_fixture_test::WakeFixture};

use super::{
    ScramProofShutdown, ScramProofWorker,
    queue_test::{assert_continues, request},
};

#[test]
fn completed_proof_wakes_the_reactor_with_exact_request_identity() {
    let mut fixture =
        WakeFixture::new().unwrap_or_else(|error| panic!("create test selector: {error}"));
    let wake = fixture.public_wake();
    let worker = ScramProofWorker::spawn(ScramProofLimits::default(), wake)
        .unwrap_or_else(|error| panic!("spawn proof worker: {error}"));
    let expected = request(7);
    let expected_debug = format!("{expected:?}");

    worker
        .sender()
        .submit(expected)
        .unwrap_or_else(|error| panic!("admit proof: {error}"));
    fixture
        .wait(Duration::from_secs(1))
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
    let mut fixture =
        WakeFixture::new().unwrap_or_else(|error| panic!("create test selector: {error}"));
    let wake = fixture.public_wake();
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
    fixture
        .wait(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("wait for first proof outcome: {error}"));

    worker
        .shutdown()
        .unwrap_or_else(|error| panic!("join capacity-blocked proof worker: {error}"));
}

#[test]
fn dropping_live_proof_owner_detaches_an_unfinished_worker() {
    let (release, exited, worker) = blocked_worker();

    assert_drop_returns(ScramProofWorker::from_worker(worker), &release, &exited);
}

#[test]
fn dropping_proof_shutdown_detaches_an_unfinished_worker() {
    let (release, exited, worker) = blocked_worker();

    assert_drop_returns(ScramProofShutdown::from_worker(worker), &release, &exited);
}

fn blocked_worker() -> (Sender<()>, Receiver<()>, thread::JoinHandle<()>) {
    let (release, blocked) = channel();
    let (finished, exited) = channel();
    let worker = thread::spawn(move || {
        let _ = blocked.recv();
        let _ = finished.send(());
    });
    (release, exited, worker)
}

fn assert_drop_returns(owner: impl Send + 'static, release: &Sender<()>, exited: &Receiver<()>) {
    let (completed, observed) = channel();
    let dropper = thread::spawn(move || {
        drop(owner);
        let _ = completed.send(());
    });

    assert_eq!(observed.recv_timeout(Duration::from_secs(1)), Ok(()));
    release
        .send(())
        .unwrap_or_else(|error| panic!("release detached proof worker: {error}"));
    assert_eq!(exited.recv_timeout(Duration::from_secs(1)), Ok(()));
    dropper
        .join()
        .unwrap_or_else(|_| panic!("join nonblocking drop observer"));
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
