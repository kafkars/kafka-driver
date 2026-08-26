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
    let (release, blocked) = channel();
    let worker = thread::spawn(move || {
        let _ = blocked.recv();
    });
    let (observed, result) = channel();
    let owner = thread::spawn(move || {
        let (commands, shutdown, mut reactor) = isolated_reactor();
        reactor.resolver_shutdown = Some(ResolverShutdown::from_worker(worker));
        let completion = shutdown
            .subscribe(|| commands.try_send_control(Command::Shutdown))
            .unwrap_or_else(|_| panic!("request isolated shutdown"));
        let first = reactor.turn(Duration::ZERO);
        let _ = observed.send(matches!(first, Ok(TurnOutcome::Progress { .. })));
        let terminal = finish_shutdown(&mut reactor);
        (first, terminal, completion.wait())
    });

    assert_eq!(result.recv_timeout(Duration::from_secs(1)), Ok(true));
    release
        .send(())
        .unwrap_or_else(|error| panic!("release blocked resolver: {error}"));
    let (first, terminal, completion) = owner
        .join()
        .unwrap_or_else(|_| panic!("join zero-wait reactor owner"));
    assert!(matches!(first, Ok(TurnOutcome::Progress { .. })));
    assert!(matches!(terminal, TurnOutcome::Shutdown { .. }));
    assert_eq!(completion, Ok(()));
}

#[test]
fn pending_worker_shutdown_bounds_a_long_host_wait() {
    let (release, blocked) = channel();
    let worker = thread::spawn(move || {
        let _ = blocked.recv();
    });
    let (observed, result) = channel();
    let owner = thread::spawn(move || {
        let (commands, shutdown, mut reactor) = isolated_reactor();
        reactor.resolver_shutdown = Some(ResolverShutdown::from_worker(worker));
        let completion = shutdown
            .subscribe(|| commands.try_send_control(Command::Shutdown))
            .unwrap_or_else(|_| panic!("request isolated shutdown"));
        let first = reactor
            .turn(Duration::ZERO)
            .unwrap_or_else(|error| panic!("begin isolated shutdown: {error}"));
        assert!(matches!(first, TurnOutcome::Progress { .. }));
        let outcome = reactor.turn(Duration::from_secs(30));
        let _ = observed.send(());
        let terminal = finish_shutdown(&mut reactor);
        (outcome, terminal, completion.wait())
    });

    assert_eq!(result.recv_timeout(Duration::from_secs(1)), Ok(()));
    release
        .send(())
        .unwrap_or_else(|error| panic!("release blocked resolver: {error}"));
    let (first, terminal, completion) = owner
        .join()
        .unwrap_or_else(|_| panic!("join bounded-wait reactor owner"));
    assert!(matches!(first, Ok(TurnOutcome::Idle)));
    assert!(matches!(terminal, TurnOutcome::Shutdown { .. }));
    assert_eq!(completion, Ok(()));
}

fn isolated_reactor() -> (
    crate::reactor::MailboxSender<Command>,
    crate::completion::ShutdownRequester,
    Reactor,
) {
    Reactor::new_test(
        &DriverLimits::default(),
        Arc::new(CallIds::new()),
        Arc::new(Observation::default()),
    )
    .unwrap_or_else(|error| panic!("build isolated reactor: {error}"))
}

fn finish_shutdown(reactor: &mut Reactor) -> TurnOutcome {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let outcome = reactor
            .turn(Duration::ZERO)
            .unwrap_or_else(|error| panic!("finish isolated shutdown: {error}"));
        if matches!(outcome, TurnOutcome::Shutdown { .. }) {
            return outcome;
        }
        assert!(
            Instant::now() < deadline,
            "released resolver did not reach terminal shutdown"
        );
        thread::yield_now();
    }
}
