//! Terminal host proof for bounded blocking-worker abandonment.

use std::{
    num::NonZeroU16,
    sync::{
        Arc,
        mpsc::{Receiver, Sender, SyncSender, TryRecvError, channel},
    },
    thread,
    time::{Duration, Instant},
};

use kafka_driver_core::{
    BootstrapSet, BrokerEndpoint, CallFailure, CallId, Delivery, DnsFailure, DnsOutcome,
    DnsRequest, HostName, Moment,
};
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};

use crate::{
    BootstrapLimits, Call, CompletionError, DriverLimits, RequestError, Route,
    api::CallIds,
    completion::{CompletionReceiver, ShutdownRequester},
    config::BootstrapConfig,
    observation::{CallTimeline, Observation},
    reactor::{
        Command, MailboxSender, ReactorBackend, TrySendError, direct_plaintext::DirectBackend,
        scram_proof::ScramProofWorker, worker_shutdown::WORKER_SHUTDOWN_GRACE,
    },
    request::{erased_request, observed_request},
};

use super::{NameResolution, Reactor, TurnOutcome};

#[test]
fn blocked_resolver_is_abandoned_without_reopening_terminal_work() {
    let mut fixture = BlockedWorkers::new();
    let (call, shutdown) = fixture.admit_call_and_shutdown();

    let first = fixture
        .reactor
        .drive_at(Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("begin blocked-resolver shutdown: {error}"));
    assert!(matches!(first, TurnOutcome::Progress { .. }));
    assert!(!fixture.reactor.is_shutdown());
    assert_eq!(shutdown.try_result(), None);
    assert_draining_once(&call);

    let grace = shutdown_grace_deadline();
    let pending = fixture
        .reactor
        .drive_at(Moment::from_nanos(grace.as_nanos() - 1))
        .unwrap_or_else(|error| panic!("drive before shutdown grace: {error}"));
    assert!(!matches!(pending, TurnOutcome::Shutdown { .. }));
    let terminal = fixture
        .reactor
        .drive_at(grace)
        .unwrap_or_else(|error| panic!("expire blocked resolver grace: {error}"));

    assert!(matches!(terminal, TurnOutcome::Shutdown { .. }));
    assert_eq!(shutdown.wait(), Ok(()));
    let calls = fixture.observation.snapshot().calls;
    assert_eq!((calls.admitted(), calls.failed()), (1, 1));
    fixture.assert_terminal_work_closed();
    let terminal_diagnostic = format!("{:?}", fixture.reactor);
    assert!(terminal_diagnostic.contains("resolver_shutdown_abandoned: 1"));
    assert!(terminal_diagnostic.contains("proof_shutdown_abandoned: 1"));

    fixture.release_late_workers();
    assert!(matches!(
        fixture
            .reactor
            .turn(Duration::ZERO)
            .unwrap_or_else(|error| panic!("drive after late DNS completion: {error}")),
        TurnOutcome::Shutdown { .. }
    ));
    assert_eq!(format!("{:?}", fixture.reactor), terminal_diagnostic);
    assert_eq!(fixture.observation.snapshot().calls, calls);
}

struct BlockedWorkers {
    commands: MailboxSender<Command>,
    shutdown: ShutdownRequester,
    reactor: Reactor,
    observation: Arc<Observation>,
    dns_requests: Receiver<DnsRequest>,
    dns_outcomes: SyncSender<DnsOutcome>,
    resolver_release: Sender<()>,
    resolver_late: Receiver<bool>,
    proof_release: Sender<()>,
    proof_late: Receiver<()>,
}

impl BlockedWorkers {
    fn new() -> Self {
        let observation = Arc::new(Observation::default());
        let (commands, shutdown, mut reactor) = Reactor::new_test(
            &DriverLimits::default(),
            Arc::new(CallIds::new()),
            Arc::clone(&observation),
        )
        .unwrap_or_else(|error| panic!("build blocked-resolver reactor: {error}"));
        reactor.backend = ReactorBackend::Direct(Box::new(
            DirectBackend::pending_plaintext_refresh_for_test(51, 7),
        ));
        let (mut resolution, dns_requests, dns_outcomes) =
            NameResolution::isolated(bootstrap(), DriverLimits::default().resolver());
        let request = dns_requests
            .try_recv()
            .unwrap_or_else(|error| panic!("take initial DNS work: {error}"));
        let (resolver_release, resolver_late, resolver) =
            blocked_resolver(request, dns_outcomes.clone());
        resolution.install_worker_for_test(resolver);
        reactor.resolution = Some(resolution);
        let (proof_release, proof_late, proof) = blocked_proof_worker();
        reactor.scram_proof = Some(ScramProofWorker::from_worker(proof));
        Self {
            commands,
            shutdown,
            reactor,
            observation,
            dns_requests,
            dns_outcomes,
            resolver_release,
            resolver_late,
            proof_release,
            proof_late,
        }
    }

    fn admit_call_and_shutdown(
        &self,
    ) -> (
        Call<Result<ApiVersionsResponse, RequestError>>,
        CompletionReceiver<()>,
    ) {
        let submitted_at = Instant::now();
        let timeout = Duration::from_secs(30);
        let timeline = CallTimeline::new(Arc::clone(&self.observation), submitted_at, timeout);
        let (call, request) = observed_request(
            CallId::from_raw(1),
            ApiVersionsRequest::default(),
            timeout,
            timeline,
        );
        self.commands
            .try_send(Command::Submit {
                route: Route::AnyBroker,
                request,
                submitted_at,
            })
            .unwrap_or_else(|_| panic!("admit pre-shutdown call"));
        let shutdown = self
            .shutdown
            .subscribe(|| self.commands.try_send_control(Command::Shutdown))
            .unwrap_or_else(|_| panic!("request blocked-resolver shutdown"));
        (call, shutdown)
    }

    fn assert_terminal_work_closed(&self) {
        assert_eq!(self.observation.worker_shutdown_abandonments(), [1, 1]);
        assert!(matches!(
            self.dns_requests.try_recv(),
            Err(TryRecvError::Disconnected)
        ));
        assert!(self.dns_outcomes.send(late_dns_outcome()).is_err());
        assert!(self.reactor.backend.direct().is_some_and(|direct| {
            direct.is_terminal() && !direct.has_endpoint_refresh_for_test()
        }));
        let (_call, request) = erased_request(
            CallId::from_raw(2),
            ApiVersionsRequest::default(),
            Duration::from_secs(1),
        );
        assert!(matches!(
            self.commands.try_send(Command::Submit {
                route: Route::AnyBroker,
                request,
                submitted_at: Instant::now(),
            }),
            Err(TrySendError::Closed(Command::Submit { .. }))
        ));
    }

    fn release_late_workers(&self) {
        self.resolver_release
            .send(())
            .unwrap_or_else(|error| panic!("release abandoned resolver: {error}"));
        self.proof_release
            .send(())
            .unwrap_or_else(|error| panic!("release abandoned proof worker: {error}"));
        assert_eq!(
            self.resolver_late.recv_timeout(Duration::from_secs(1)),
            Ok(true)
        );
        assert_eq!(self.proof_late.recv_timeout(Duration::from_secs(1)), Ok(()));
    }
}

fn assert_draining_once(call: &Call<Result<ApiVersionsResponse, RequestError>>) {
    assert!(matches!(
        call.try_result(),
        Some(Ok(Err(RequestError::Rejected {
            failure: CallFailure::Draining,
            delivery: Delivery::NotSent,
        })))
    ));
    assert_eq!(call.try_result(), Some(Err(CompletionError::Consumed)));
}

fn blocked_resolver(
    request: DnsRequest,
    outcomes: SyncSender<DnsOutcome>,
) -> (Sender<()>, Receiver<bool>, thread::JoinHandle<()>) {
    let (release, blocked) = channel();
    let (finished, observed) = channel();
    let worker = thread::spawn(move || {
        let _ = blocked.recv();
        let rejected = outcomes
            .send(DnsOutcome::new(
                request.epoch(),
                request.effect_id(),
                Err(DnsFailure::Temporary),
            ))
            .is_err();
        let _ = finished.send(rejected);
    });
    (release, observed, worker)
}

fn blocked_proof_worker() -> (Sender<()>, Receiver<()>, thread::JoinHandle<()>) {
    let (release, blocked) = channel();
    let (finished, observed) = channel();
    let worker = thread::spawn(move || {
        let _ = blocked.recv();
        let _ = finished.send(());
    });
    (release, observed, worker)
}

fn late_dns_outcome() -> DnsOutcome {
    DnsOutcome::new(
        kafka_driver_core::ConnectionEpoch::from_raw(1),
        kafka_driver_core::EffectId::from_raw(1),
        Err(DnsFailure::Temporary),
    )
}

fn shutdown_grace_deadline() -> Moment {
    Moment::ORIGIN
        .checked_add(WORKER_SHUTDOWN_GRACE)
        .unwrap_or_else(|| panic!("shutdown grace must fit test time"))
}

fn bootstrap() -> BootstrapConfig {
    let endpoint = BrokerEndpoint::new(
        HostName::new("blocked-resolver.test")
            .unwrap_or_else(|error| panic!("valid blocked resolver hostname: {error}")),
        NonZeroU16::new(9092).unwrap_or_else(|| panic!("test port must be nonzero")),
    );
    let endpoints = BootstrapSet::try_from_iter([endpoint], BootstrapLimits::default())
        .unwrap_or_else(|error| panic!("valid blocked resolver bootstrap: {error}"));
    BootstrapConfig::plaintext(endpoints)
}
