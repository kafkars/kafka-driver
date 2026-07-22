//! Public scenarios for bounded shutdown, wake, and terminal reactor ownership.

mod support;

use std::{
    future::Future,
    net::TcpListener,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};

use kafka_driver::{
    Delivery, Driver, DriverLimits, Reactor, RequestError, SubmitError, TurnOutcome,
};
use kafka_driver_core::CallFailure;
use kafka_wire::ApiVersionsRequest;

use support::complete_negotiation;

#[test]
fn embedded_shutdown_completes_once_and_closes_admission() {
    let (driver, mut reactor, _listener) = build_reactor(DriverLimits::default());
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
    let (driver, mut reactor, _listener) = build_reactor(limits);
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
fn full_request_mailbox_cannot_reject_or_delay_shutdown() {
    let limits = DriverLimits::new(NonZeroUsize::MIN, NonZeroUsize::MIN);
    let (driver, mut reactor, _listener) = build_reactor(limits);
    let request = driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("fill request mailbox: {error}"));

    let shutdown = driver.shutdown();
    let outcome = reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("drive priority shutdown: {error}"));

    assert_eq!(outcome, TurnOutcome::Shutdown { commands: 1 });
    let shutdown = shutdown.unwrap_or_else(|error| panic!("admit shutdown control: {error}"));
    assert_eq!(shutdown.wait(), Ok(()));
    assert_eq!(
        request.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::Draining,
            delivery: Delivery::NotSent,
        }))
    );
}

#[test]
fn cross_thread_admission_wakes_a_blocked_reactor_turn() {
    let (driver, mut reactor, listener) = build_reactor(DriverLimits::default());
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept lifecycle connection: {error}"));
    complete_negotiation(&mut peer, &mut reactor);
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
    let (_driver, mut reactor, listener) = build_reactor(DriverLimits::default());
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept lifecycle connection: {error}"));
    complete_negotiation(&mut peer, &mut reactor);
    let wake = reactor.wake_handle();
    assert!(wake.wake().is_ok());

    let Ok(outcome) = reactor.turn(Duration::from_secs(30)) else {
        panic!("reactor turn must succeed");
    };

    assert_eq!(outcome, TurnOutcome::Idle);
}

#[test]
fn dropping_the_last_driver_handle_wakes_and_closes_the_reactor() {
    let (driver, mut reactor, listener) = build_reactor(DriverLimits::default());
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept lifecycle connection: {error}"));
    complete_negotiation(&mut peer, &mut reactor);
    let owner = thread::spawn(move || reactor.turn(Duration::from_secs(30)));

    drop(driver);

    assert!(matches!(
        owner.join(),
        Ok(Ok(TurnOutcome::Shutdown { commands: 0 }))
    ));
}

#[test]
fn generated_call_before_broker_readiness_is_not_sent_or_left_pending() {
    let (driver, mut reactor, _listener) = build_reactor(DriverLimits::default());
    let call = driver.call(ApiVersionsRequest::default(), Duration::from_secs(1));
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

#[test]
fn generated_call_reaches_a_ready_configured_broker_owner() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback broker address: {error}"));
    let Ok((driver, mut reactor)) = Driver::builder().broker(address).build_reactor() else {
        panic!("host must create a configured broker reactor");
    };
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept broker connection: {error}"));
    complete_negotiation(&mut peer, &mut reactor);
    let Ok(mut call) = driver.call(ApiVersionsRequest::default(), Duration::from_secs(1)) else {
        panic!("generated call must enter the configured mailbox");
    };

    let Ok(admitted) = reactor.turn(Duration::ZERO) else {
        panic!("reactor must admit the generated call");
    };

    assert_eq!(
        admitted,
        TurnOutcome::Progress {
            commands: 1,
            more_work: false,
        }
    );
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Future::poll(Pin::new(&mut call), &mut context),
        Poll::Pending
    ));
}

fn build_reactor(limits: DriverLimits) -> (Driver, Reactor, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind lifecycle broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read lifecycle broker address: {error}"));
    let Ok((driver, reactor)) = Driver::builder()
        .limits(limits)
        .broker(address)
        .build_reactor()
    else {
        panic!("host must provide a Mio selector");
    };
    (driver, reactor, listener)
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}
