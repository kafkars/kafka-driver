//! Focused real-worker scenario for identity, wake, and numeric address fidelity.

use std::{
    num::{NonZeroU16, NonZeroUsize},
    sync::mpsc::{Receiver, Sender, channel},
    thread,
    time::Duration,
};

use kafka_driver_core::{
    BrokerEndpoint, ConnectionEpoch, DnsRequest, EffectId, HostName, IpAddress,
};

use crate::{ResolverLimits, reactor::Poller};

use super::{Resolver, ResolverShutdown};

#[test]
fn numeric_resolution_wakes_the_reactor_with_exact_request_identity() {
    let mut poller = Poller::new(std::num::NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("create test poller: {error}"));
    let wake = crate::reactor::WakeHandle::new(poller.pulse_handle());
    let resolver = Resolver::spawn(ResolverLimits::default(), wake)
        .unwrap_or_else(|error| panic!("spawn DNS worker: {error}"));
    let request = DnsRequest::new(
        ConnectionEpoch::from_raw(7),
        EffectId::from_raw(11),
        endpoint(),
    );

    resolver
        .submit(request.clone())
        .unwrap_or_else(|error| panic!("admit DNS request: {error}"));
    let mut events = Vec::new();
    poller
        .poll_into(Some(Duration::from_secs(1)), &mut events)
        .unwrap_or_else(|error| panic!("wait for DNS worker wake: {error}"));
    let mut outcomes = Vec::new();
    let progress = resolver
        .drain_into(&mut outcomes)
        .unwrap_or_else(|error| panic!("drain numeric DNS outcome: {error}"));

    assert_eq!(progress.outcomes(), 1);
    assert!(!progress.more_work());
    assert_eq!(outcomes[0].epoch(), request.epoch());
    assert_eq!(outcomes[0].effect_id(), request.effect_id());
    let addresses = outcomes[0]
        .result()
        .as_ref()
        .unwrap_or_else(|failure| panic!("numeric resolution must succeed: {failure:?}"));
    assert_eq!(addresses.len(), 1);
    assert_eq!(
        addresses.iter().next().map(|address| address.ip()),
        Some(IpAddress::V4([127, 0, 0, 1]))
    );
    resolver
        .shutdown()
        .unwrap_or_else(|error| panic!("join DNS worker: {error}"));
}

#[test]
fn shutdown_joins_a_worker_blocked_by_full_outcome_capacity() {
    let mut poller = Poller::new(NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("create test poller: {error}"));
    let wake = crate::reactor::WakeHandle::new(poller.pulse_handle());
    let limits = ResolverLimits::new(
        nonzero(2),
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
    );
    let resolver =
        Resolver::spawn(limits, wake).unwrap_or_else(|error| panic!("spawn DNS worker: {error}"));
    for raw in 1..=2 {
        resolver
            .submit(DnsRequest::new(
                ConnectionEpoch::from_raw(1),
                EffectId::from_raw(raw),
                endpoint(),
            ))
            .unwrap_or_else(|error| panic!("admit DNS request: {error}"));
    }
    let mut events = Vec::new();
    poller
        .poll_into(Some(Duration::from_secs(1)), &mut events)
        .unwrap_or_else(|error| panic!("wait for first DNS outcome: {error}"));

    resolver
        .shutdown()
        .unwrap_or_else(|error| panic!("join capacity-blocked DNS worker: {error}"));
}

#[test]
fn shutdown_poll_returns_while_the_worker_is_still_blocked() {
    let (release, blocked) = channel();
    let worker = thread::spawn(move || {
        let _ = blocked.recv();
    });
    let mut shutdown = ResolverShutdown::from_worker(worker);
    let (observed, result) = channel();
    let poll = thread::spawn(move || {
        let progress = shutdown.poll_complete();
        let _ = observed.send(progress);
        shutdown
    });

    assert!(matches!(
        result.recv_timeout(Duration::from_secs(1)),
        Ok(Ok(false))
    ));
    release
        .send(())
        .unwrap_or_else(|error| panic!("release blocked resolver worker: {error}"));
    shutdown = poll
        .join()
        .unwrap_or_else(|_| panic!("join resolver shutdown poll"));
    while !shutdown
        .poll_complete()
        .unwrap_or_else(|error| panic!("finish resolver shutdown: {error}"))
    {
        thread::yield_now();
    }
}

#[test]
fn dropping_live_resolver_detaches_an_unfinished_worker() {
    let (release, exited, worker) = blocked_worker();

    assert_drop_returns(Resolver::from_worker(worker), &release, &exited);
}

#[test]
fn dropping_resolver_shutdown_detaches_an_unfinished_worker() {
    let (release, exited, worker) = blocked_worker();

    assert_drop_returns(ResolverShutdown::from_worker(worker), &release, &exited);
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
        .unwrap_or_else(|error| panic!("release detached resolver worker: {error}"));
    assert_eq!(exited.recv_timeout(Duration::from_secs(1)), Ok(()));
    dropper
        .join()
        .unwrap_or_else(|_| panic!("join nonblocking drop observer"));
}

fn endpoint() -> BrokerEndpoint {
    let host = HostName::new("127.0.0.1")
        .unwrap_or_else(|error| panic!("numeric host must be valid: {error}"));
    BrokerEndpoint::new(host, port())
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
