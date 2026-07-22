//! Public scenarios for bounded shutdown, wake, and terminal reactor ownership.

use std::{num::NonZeroUsize, thread, time::Duration};

use kafka_driver::{Driver, DriverLimits, SubmitError, TurnOutcome};

#[test]
fn embedded_shutdown_completes_once_and_closes_admission() {
    let (driver, mut reactor) = Driver::builder().build_reactor();
    let call = driver.shutdown();
    assert!(call.is_ok());

    let outcome = reactor.turn(Duration::ZERO);

    assert_eq!(outcome, TurnOutcome::Shutdown { commands: 1 });
    assert!(reactor.is_shutdown());
    let Ok(call) = call else {
        panic!("the admitted shutdown call must be present");
    };
    assert_eq!(call.wait(), Ok(()));
    assert!(matches!(driver.shutdown(), Err(SubmitError::Closed)));
}

#[test]
fn mailbox_capacity_rejects_a_second_pending_shutdown() {
    let limits = DriverLimits::new(NonZeroUsize::MIN, NonZeroUsize::MIN);
    let (driver, mut reactor) = Driver::builder().limits(limits).build_reactor();
    let first = driver.shutdown();

    let second = driver.shutdown();

    assert!(matches!(second, Err(SubmitError::Full)));
    assert_eq!(
        reactor.turn(Duration::ZERO),
        TurnOutcome::Shutdown { commands: 1 }
    );
    let Ok(first) = first else {
        panic!("the first shutdown must be admitted");
    };
    assert_eq!(first.wait(), Ok(()));
}

#[test]
fn cross_thread_admission_wakes_a_blocked_reactor_turn() {
    let (driver, mut reactor) = Driver::builder().build_reactor();
    let owner = thread::spawn(move || reactor.turn(Duration::from_secs(30)));

    let call = driver.shutdown();

    assert!(call.is_ok());
    assert!(matches!(
        owner.join(),
        Ok(TurnOutcome::Shutdown { commands: 1 })
    ));
}

#[test]
fn explicit_wake_releases_an_idle_embedded_turn() {
    let (_driver, mut reactor) = Driver::builder().build_reactor();
    let wake = reactor.wake_handle();
    wake.wake();

    let outcome = reactor.turn(Duration::from_secs(30));

    assert_eq!(outcome, TurnOutcome::Idle);
}

#[test]
fn dropping_the_last_driver_handle_wakes_and_closes_the_reactor() {
    let (driver, mut reactor) = Driver::builder().build_reactor();
    let owner = thread::spawn(move || reactor.turn(Duration::from_secs(30)));

    drop(driver);

    assert!(matches!(
        owner.join(),
        Ok(TurnOutcome::Shutdown { commands: 0 })
    ));
}
