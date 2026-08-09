//! Host-level proof that zero-wait turns never join a blocked worker.

use std::{
    sync::{Arc, mpsc::channel},
    thread,
    time::{Duration, Instant},
};

use crate::{
    DriverLimits,
    api::CallIds,
    observation::Observation,
    reactor::{Command, resolver::ResolverShutdown},
};

use super::{Reactor, TurnOutcome};

#[test]
fn zero_wait_turn_returns_while_resolver_shutdown_is_blocked() {
    let (commands, shutdown, mut reactor) = Reactor::new(
        &DriverLimits::default(),
        None,
        Arc::new(CallIds::new()),
        Arc::new(Observation::default()),
    )
    .unwrap_or_else(|error| panic!("build isolated reactor: {error}"));
    let (release, blocked) = channel();
    let worker = thread::spawn(move || {
        let _ = blocked.recv();
    });
    reactor.resolver_shutdown = Some(ResolverShutdown::from_worker(worker));
    let completion = shutdown
        .subscribe(|| commands.try_send_control(Command::Shutdown))
        .unwrap_or_else(|_| panic!("request isolated shutdown"));
    let (observed, result) = channel();
    let turn = thread::spawn(move || {
        let outcome = reactor.turn(Duration::ZERO);
        let _ = observed.send(matches!(outcome, Ok(TurnOutcome::Progress { .. })));
        (reactor, outcome)
    });

    assert_eq!(result.recv_timeout(Duration::from_secs(1)), Ok(true));
    release
        .send(())
        .unwrap_or_else(|error| panic!("release blocked resolver: {error}"));
    let (mut reactor, first) = turn
        .join()
        .unwrap_or_else(|_| panic!("join zero-wait reactor turn"));
    assert!(matches!(first, Ok(TurnOutcome::Progress { .. })));
    let deadline = Instant::now() + Duration::from_secs(1);
    let terminal = loop {
        let outcome = reactor
            .turn(Duration::ZERO)
            .unwrap_or_else(|error| panic!("finish isolated shutdown: {error}"));
        if matches!(outcome, TurnOutcome::Shutdown { .. }) {
            break outcome;
        }
        assert!(
            Instant::now() < deadline,
            "released resolver did not reach terminal shutdown"
        );
        thread::yield_now();
    };

    assert!(matches!(terminal, TurnOutcome::Shutdown { .. }));
    assert_eq!(completion.wait(), Ok(()));
}

#[test]
fn pending_worker_shutdown_bounds_a_long_host_wait() {
    let (commands, shutdown, mut reactor) = Reactor::new(
        &DriverLimits::default(),
        None,
        Arc::new(CallIds::new()),
        Arc::new(Observation::default()),
    )
    .unwrap_or_else(|error| panic!("build isolated reactor: {error}"));
    let (release, blocked) = channel();
    let worker = thread::spawn(move || {
        let _ = blocked.recv();
    });
    reactor.resolver_shutdown = Some(ResolverShutdown::from_worker(worker));
    let completion = shutdown
        .subscribe(|| commands.try_send_control(Command::Shutdown))
        .unwrap_or_else(|_| panic!("request isolated shutdown"));
    let first = reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("begin isolated shutdown: {error}"));
    assert!(matches!(first, TurnOutcome::Progress { .. }));
    let (observed, result) = channel();
    let turn = thread::spawn(move || {
        let outcome = reactor.turn(Duration::from_secs(30));
        let _ = observed.send(());
        (reactor, outcome)
    });

    assert_eq!(result.recv_timeout(Duration::from_secs(1)), Ok(()));
    release
        .send(())
        .unwrap_or_else(|error| panic!("release blocked resolver: {error}"));
    let (mut reactor, first) = turn
        .join()
        .unwrap_or_else(|_| panic!("join bounded-wait reactor turn"));
    assert!(matches!(first, Ok(TurnOutcome::Idle)));
    let deadline = Instant::now() + Duration::from_secs(1);
    while !reactor.is_shutdown() {
        reactor
            .turn(Duration::ZERO)
            .unwrap_or_else(|error| panic!("finish isolated shutdown: {error}"));
        assert!(
            Instant::now() < deadline,
            "released resolver did not reach terminal shutdown"
        );
        thread::yield_now();
    }

    assert_eq!(completion.wait(), Ok(()));
}
