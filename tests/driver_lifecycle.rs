//! Public scenarios for bounded shutdown, wake, and terminal reactor ownership.

use std::{num::NonZeroUsize, thread, time::Duration};

use kafka_driver::{
    ApiVersion, Delivery, Driver, DriverLimits, Reactor, RequestError, SubmitError, TurnOutcome,
};
use kafka_driver_core::CallFailure;
use kafka_wire::ApiVersionsRequest;

#[test]
fn embedded_shutdown_completes_once_and_closes_admission() {
    let (driver, mut reactor) = build_reactor(DriverLimits::default());
    let call = driver.shutdown();
    assert!(call.is_ok());

    let Ok(outcome) = reactor.turn(Duration::ZERO) else {
        panic!("reactor turn must succeed");
    };

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
    let (driver, mut reactor) = build_reactor(limits);
    let first = driver.shutdown();

    let second = driver.shutdown();

    assert!(matches!(second, Err(SubmitError::Full)));
    let Ok(outcome) = reactor.turn(Duration::ZERO) else {
        panic!("reactor turn must succeed");
    };
    assert_eq!(outcome, TurnOutcome::Shutdown { commands: 1 });
    let Ok(first) = first else {
        panic!("the first shutdown must be admitted");
    };
    assert_eq!(first.wait(), Ok(()));
}

#[test]
fn cross_thread_admission_wakes_a_blocked_reactor_turn() {
    let (driver, mut reactor) = build_reactor(DriverLimits::default());
    let owner = thread::spawn(move || reactor.turn(Duration::from_secs(30)));

    let call = driver.shutdown();

    assert!(call.is_ok());
    assert!(matches!(
        owner.join(),
        Ok(Ok(TurnOutcome::Shutdown { commands: 1 }))
    ));
}

#[test]
fn explicit_wake_releases_an_idle_embedded_turn() {
    let (_driver, mut reactor) = build_reactor(DriverLimits::default());
    let wake = reactor.wake_handle();
    assert!(wake.wake().is_ok());

    let Ok(outcome) = reactor.turn(Duration::from_secs(30)) else {
        panic!("reactor turn must succeed");
    };

    assert_eq!(outcome, TurnOutcome::Idle);
}

#[test]
fn dropping_the_last_driver_handle_wakes_and_closes_the_reactor() {
    let (driver, mut reactor) = build_reactor(DriverLimits::default());
    let owner = thread::spawn(move || reactor.turn(Duration::from_secs(30)));

    drop(driver);

    assert!(matches!(
        owner.join(),
        Ok(Ok(TurnOutcome::Shutdown { commands: 0 }))
    ));
}

#[test]
fn generated_call_before_broker_configuration_is_not_sent_or_left_pending() {
    let (driver, mut reactor) = build_reactor(DriverLimits::default());
    let call = driver.call(
        ApiVersionsRequest::default(),
        ApiVersion::new(0),
        Duration::from_secs(1),
    );
    let Ok(call) = call else {
        panic!("request command must enter the mailbox");
    };

    let Ok(outcome) = reactor.turn(Duration::ZERO) else {
        panic!("reactor turn must succeed");
    };

    assert_eq!(
        outcome,
        TurnOutcome::Progress {
            commands: 1,
            more_work: false,
        }
    );
    assert_eq!(
        call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::NotReady,
            delivery: Delivery::NotSent,
        }))
    );
}

fn build_reactor(limits: DriverLimits) -> (Driver, Reactor) {
    let Ok(pair) = Driver::builder().limits(limits).build_reactor() else {
        panic!("host must provide a Mio selector");
    };
    pair
}
