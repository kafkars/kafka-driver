//! Shared shutdown subscription scenarios independent of reactor scheduling.

use std::{cell::Cell, num::NonZeroUsize};

use super::{CompletionError, ShutdownSubscribeError, shutdown_barrier};

#[test]
fn one_request_admits_every_bounded_subscriber_to_the_same_terminal_outcome() {
    let (requester, mut completer) = shutdown_barrier(nonzero(2));
    let requests = Cell::new(0);
    let first = requester
        .subscribe(|| {
            requests.set(requests.get() + 1);
            Ok::<_, ()>(())
        })
        .unwrap_or_else(|_| panic!("admit first shutdown subscriber"));
    let second = requester
        .subscribe(|| {
            requests.set(requests.get() + 1);
            Ok::<_, ()>(())
        })
        .unwrap_or_else(|_| panic!("admit second shutdown subscriber"));

    completer.complete();

    assert_eq!(requests.get(), 1);
    assert_eq!(first.wait(), Ok(()));
    assert_eq!(second.wait(), Ok(()));
    let completed = requester
        .subscribe(|| -> Result<(), ()> { panic!("completed barrier must not request again") })
        .unwrap_or_else(|_| panic!("subscribe to completed shutdown"));
    assert_eq!(completed.wait(), Ok(()));
}

#[test]
fn subscriber_capacity_is_atomic_and_does_not_admit_an_extra_command() {
    let (requester, _completer) = shutdown_barrier(NonZeroUsize::MIN);
    let first = requester.subscribe(|| Ok::<_, ()>(()));

    let second =
        requester.subscribe(|| -> Result<(), ()> { panic!("follower must not request shutdown") });

    assert!(first.is_ok());
    assert!(matches!(second, Err(ShutdownSubscribeError::Full)));
}

#[test]
fn failed_first_request_leaves_the_barrier_open_for_a_later_attempt() {
    let (requester, mut completer) = shutdown_barrier(NonZeroUsize::MIN);
    let failed = requester.subscribe(|| Err("wake failed"));
    assert!(matches!(
        failed,
        Err(ShutdownSubscribeError::Request("wake failed"))
    ));
    let retried = requester
        .subscribe(|| Ok::<_, ()>(()))
        .unwrap_or_else(|_| panic!("retry first shutdown request"));

    completer.complete();

    assert_eq!(retried.wait(), Ok(()));
}

#[test]
fn dropping_terminal_ownership_closes_every_successful_subscriber() {
    let (requester, completer) = shutdown_barrier(nonzero(2));
    let first = requester
        .subscribe(|| Ok::<_, ()>(()))
        .unwrap_or_else(|_| panic!("admit shutdown subscriber"));
    let second = requester
        .subscribe(|| Ok::<_, ()>(()))
        .unwrap_or_else(|_| panic!("admit shutdown follower"));

    drop(completer);

    assert_eq!(first.wait(), Err(CompletionError::Closed));
    assert_eq!(second.wait(), Err(CompletionError::Closed));
    assert!(matches!(
        requester.subscribe(|| Ok::<_, ()>(())),
        Err(ShutdownSubscribeError::Closed)
    ));
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test capacity must be nonzero"))
}
