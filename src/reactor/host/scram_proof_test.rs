//! Direct-host proof wake, delivery, and nonblocking shutdown scenarios.

use std::{
    net::{TcpListener, TcpStream},
    num::NonZeroUsize,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use calandria::{Span, WaitOutcome};
use kafka_driver_core::{AuthenticationRound, EffectId};

use crate::{
    DriverLimits, SaslConfig, ScramProofLimits,
    api::CallIds,
    config::{BrokerConfig, DriverTarget},
    observation::Observation,
};

use super::Reactor;
use crate::reactor::direct_plaintext::{DirectBackend, scram_fixture_test::NOW};

const EFFECT: EffectId = EffectId::from_raw(3);

#[test]
fn direct_worker_uses_bornera_wake_and_delivers_the_exact_proof() {
    let (mut reactor, _peer) = direct_scram_reactor(&DriverLimits::default());
    assert!(reactor.backend.direct().is_some());
    assert!(reactor.scram_proof.is_some());
    assert_eq!(reactor.backend.selector_count(), 1);
    let direct = reactor
        .backend
        .direct_mut()
        .unwrap_or_else(|| panic!("SCRAM target must select Direct"));
    assert!(direct.has_scram_sender_for_test());
    let fence = direct.arm_scram_proof_for_test(EFFECT);
    assert!(fence.target().direct_connection().is_some());

    let wait = Span::try_from(Duration::from_secs(1)).unwrap_or(Span::ZERO);
    let observed = reactor
        .wait_for_events(wait)
        .unwrap_or_else(|error| panic!("wait for Direct proof wake: {error}"));
    assert_eq!(observed, WaitOutcome::Notified);
    let turn = reactor
        .continue_scram_proofs(NOW)
        .unwrap_or_else(|error| panic!("deliver hosted Direct proof: {error}"));

    assert!(turn.made_progress());
    assert_eq!(
        reactor
            .backend
            .direct()
            .and_then(DirectBackend::scram_round_for_test)
            .map(AuthenticationRound::get),
        Some(2)
    );
    stop_worker(&mut reactor);
}

#[test]
fn shutdown_with_an_outstanding_direct_proof_never_joins_inline() {
    let one = NonZeroUsize::MIN;
    let limits =
        DriverLimits::default().with_scram_proof_limits(ScramProofLimits::new(one, one, one));
    let (mut reactor, _peer) = direct_scram_reactor(&limits);
    reactor
        .backend
        .direct_mut()
        .unwrap_or_else(|| panic!("SCRAM target must select Direct"))
        .arm_scram_proof_for_test(EFFECT);
    assert!(
        reactor
            .backend
            .direct()
            .is_some_and(DirectBackend::has_pending_scram_proof_for_test)
    );

    let started = Instant::now();
    reactor
        .begin_implicit_shutdown(NOW)
        .unwrap_or_else(|error| panic!("begin Direct proof shutdown: {error}"));

    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(reactor.scram_proof.is_none());
    assert!(reactor.scram_proof_shutdown.is_some());
    assert!(reactor.backend.direct().is_some_and(|direct| {
        !direct.has_scram_sender_for_test() && !direct.has_pending_scram_proof_for_test()
    }));
    finish_worker_shutdown(&mut reactor);
}

fn direct_scram_reactor(limits: &DriverLimits) -> (Reactor, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind hosted Direct SCRAM broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read hosted Direct SCRAM address: {error}"));
    let sasl = SaslConfig::scram_sha_256("host-user", "host-password")
        .unwrap_or_else(|error| panic!("construct hosted Direct SCRAM config: {error}"));
    let target = DriverTarget::Direct(BrokerConfig::plaintext(address).with_sasl(Some(sasl)));
    let (_commands, _shutdown, reactor) = Reactor::new(
        limits,
        Some(target),
        Arc::new(CallIds::new()),
        Arc::new(Observation::default()),
    )
    .unwrap_or_else(|error| panic!("construct hosted Direct SCRAM reactor: {error}"));
    let (peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept hosted Direct SCRAM connection: {error}"));
    (reactor, peer)
}

fn stop_worker(reactor: &mut Reactor) {
    reactor
        .backend
        .direct_mut()
        .unwrap_or_else(|| panic!("Direct backend missing during proof cleanup"))
        .release_scram_proof_sender();
    reactor
        .scram_proof
        .take()
        .unwrap_or_else(|| panic!("Direct proof worker missing during cleanup"))
        .shutdown()
        .unwrap_or_else(|error| panic!("join hosted Direct proof worker: {error}"));
}

fn finish_worker_shutdown(reactor: &mut Reactor) {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let stopped = reactor
            .scram_proof_shutdown
            .as_mut()
            .unwrap_or_else(|| panic!("Direct proof shutdown owner missing"))
            .poll_complete()
            .unwrap_or_else(|error| panic!("poll Direct proof shutdown: {error}"));
        if stopped {
            reactor.scram_proof_shutdown = None;
            return;
        }
        assert!(
            Instant::now() < deadline,
            "Direct proof worker did not stop after sender release"
        );
        thread::yield_now();
    }
}
