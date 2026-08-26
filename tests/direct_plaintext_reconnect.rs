//! Public numeric plaintext reconnection through one long-lived Bornera owner.

#[path = "support/sasl_broker.rs"]
mod sasl_broker;

use std::{
    io::ErrorKind,
    net::{SocketAddr, TcpListener, TcpStream},
    time::Duration,
};

use kafka_driver::{
    BrokerState, Call, CallFailure, ConnectionCloseReason, ConnectionPhase, Delivery, Driver,
    DriverSnapshot, NegotiationFailure, Reactor, RequestError, TurnOutcome,
};
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};

use sasl_broker::SaslPeer;

#[test]
fn queued_call_survives_negotiation_loss_and_completes_on_epoch_two() {
    let mut scenario = BackoffScenario::new();
    let mut second = scenario.broker.accept_generation(&mut scenario.reactor);

    second.drive_until_frame(&mut scenario.reactor);
    let negotiation = second.expect_negotiation();
    assert_eq!(negotiation, 0, "fresh epoch must restart negotiation");
    assert!(scenario.call.try_result().is_none());
    second.respond_to_negotiation(negotiation);

    second.drive_until_frame(&mut scenario.reactor);
    let public_correlation = second.expect_generated_call();
    assert!(scenario.call.try_result().is_none());
    second.respond_to_generated_call(public_correlation);

    assert_eq!(
        drive_call(&mut scenario.reactor, &scenario.call),
        Ok(Ok(ApiVersionsResponse::default()))
    );
    let snapshot = current_snapshot(&scenario.driver, &mut scenario.reactor);
    let seed = snapshot
        .seed()
        .unwrap_or_else(|| panic!("reconnected direct seed must remain observable"));
    assert!(matches!(
        seed.broker_state(),
        BrokerState::Available { epoch } if epoch.get() == 2
    ));
    assert_eq!(seed.connection_phase(), ConnectionPhase::Ready);
    assert_eq!(seed.last_close_reason(), Some(scenario.close_reason));
    assert_eq!(snapshot.calls().admitted(), 1);
    assert_eq!(snapshot.calls().succeeded(), 1);
    assert_eq!(snapshot.calls().failed(), 0);
}

#[test]
fn accepted_epoch_two_call_uses_its_own_transport_close_reason() {
    let mut scenario = BackoffScenario::new();
    let mut second = scenario.broker.accept_generation(&mut scenario.reactor);
    second.drive_until_frame(&mut scenario.reactor);
    let negotiation = second.expect_negotiation();
    second.respond_to_negotiation(negotiation);
    second.drive_until_frame(&mut scenario.reactor);
    let _accepted = second.expect_generated_call();
    assert!(scenario.call.try_result().is_none());
    drop(second);

    let result = drive_call(&mut scenario.reactor, &scenario.call);
    let Ok(Err(RequestError::Rejected {
        failure: CallFailure::ConnectionClosed { reason },
        delivery,
    })) = result
    else {
        panic!("accepted epoch-two call must fail with its own connection close: {result:?}");
    };
    assert!(matches!(reason, ConnectionCloseReason::TransportLost(_)));
    assert_ne!(reason, scenario.close_reason);
    assert_eq!(delivery, Delivery::PossiblySent);
}

#[test]
fn shutdown_during_backoff_fails_the_unsent_call() {
    let mut scenario = BackoffScenario::new();
    let shutdown = scenario
        .driver
        .shutdown()
        .unwrap_or_else(|error| panic!("admit shutdown during direct backoff: {error}"));

    drive_until_shutdown(&mut scenario.reactor);

    assert_eq!(shutdown.wait(), Ok(()));
    assert_eq!(
        scenario.call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::Draining,
            delivery: Delivery::NotSent,
        }))
    );
}

struct BackoffScenario {
    broker: TwoGenerationBroker,
    driver: Driver,
    reactor: Reactor,
    call: Call<Result<ApiVersionsResponse, RequestError>>,
    close_reason: ConnectionCloseReason,
}

impl BackoffScenario {
    fn new() -> Self {
        let broker = TwoGenerationBroker::bind();
        let (driver, mut reactor) = Driver::builder()
            .broker(broker.address())
            .build_reactor()
            .unwrap_or_else(|error| panic!("build reconnecting direct reactor: {error}"));
        let call = driver
            .call(ApiVersionsRequest::default(), Duration::from_secs(30))
            .unwrap_or_else(|error| panic!("admit call before direct readiness: {error}"));

        let mut first = broker.accept_generation(&mut reactor);
        first.drive_until_frame(&mut reactor);
        assert_eq!(first.expect_negotiation(), 0);
        assert!(call.try_result().is_none());
        drop(first);

        reactor
            .turn(Duration::from_secs(1))
            .unwrap_or_else(|error| panic!("observe first-generation disconnect: {error}"));
        let snapshot = backoff_snapshot(&driver, &mut reactor);
        let seed = snapshot
            .seed()
            .unwrap_or_else(|| panic!("backing-off direct seed must remain observable"));
        let (failed_epoch, next_epoch) = match seed.broker_state() {
            BrokerState::Backoff {
                failed_epoch,
                next_epoch,
                ..
            } => (failed_epoch, next_epoch),
            state => panic!("first transport loss must enter backoff, observed {state:?}"),
        };
        assert_eq!(failed_epoch.get(), 1);
        assert_eq!(next_epoch.get(), 2);
        assert_eq!(seed.connection_phase(), ConnectionPhase::Closed);
        let close_reason = seed
            .last_close_reason()
            .unwrap_or_else(|| panic!("backoff must retain the first close reason"));
        assert_eq!(
            close_reason,
            ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Malformed),
            "losing the transport before ApiVersions replies must retain the semantic negotiation failure",
        );
        assert!(call.try_result().is_none());

        Self {
            broker,
            driver,
            reactor,
            call,
            close_reason,
        }
    }
}

struct TwoGenerationBroker {
    listener: TcpListener,
}

impl TwoGenerationBroker {
    fn bind() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|error| panic!("bind reconnect broker: {error}"));
        listener
            .set_nonblocking(true)
            .unwrap_or_else(|error| panic!("make reconnect listener nonblocking: {error}"));
        Self { listener }
    }

    fn address(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .unwrap_or_else(|error| panic!("read reconnect broker address: {error}"))
    }

    fn accept_generation(&self, reactor: &mut Reactor) -> SaslPeer<TcpStream> {
        for _ in 0..32 {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(1)))
                        .unwrap_or_else(|error| panic!("bound reconnect broker read: {error}"));
                    return SaslPeer::new(stream);
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Err(error) => panic!("accept reconnect generation: {error}"),
            }
            reactor
                .turn(Duration::from_millis(100))
                .unwrap_or_else(|error| panic!("drive reconnect generation: {error}"));
        }
        panic!("direct owner did not open the next generation: {reactor:?}");
    }
}

fn backoff_snapshot(driver: &Driver, reactor: &mut Reactor) -> DriverSnapshot {
    for _ in 0..8 {
        let snapshot = current_snapshot(driver, reactor);
        if snapshot
            .seed()
            .is_some_and(|seed| matches!(seed.broker_state(), BrokerState::Backoff { .. }))
        {
            return snapshot;
        }
    }
    panic!("first direct generation never became observably backed off");
}

fn current_snapshot(driver: &Driver, reactor: &mut Reactor) -> DriverSnapshot {
    let snapshot = driver
        .snapshot()
        .unwrap_or_else(|error| panic!("admit direct reconnect snapshot: {error}"));
    reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("interpret direct reconnect snapshot: {error}"));
    snapshot
        .wait()
        .unwrap_or_else(|error| panic!("receive direct reconnect snapshot: {error}"))
        .unwrap_or_else(|error| panic!("build direct reconnect snapshot: {error}"))
}

fn drive_call<T>(
    reactor: &mut Reactor,
    call: &Call<T>,
) -> Result<T, kafka_driver::CompletionError> {
    for _ in 0..64 {
        if let Some(result) = call.try_result() {
            return result;
        }
        reactor
            .turn(Duration::from_millis(25))
            .unwrap_or_else(|error| panic!("drive reconnected call: {error}"));
    }
    panic!("reconnected call remained pending after bounded turns");
}

fn drive_until_shutdown(reactor: &mut Reactor) {
    for _ in 0..8 {
        let outcome = reactor
            .turn(Duration::ZERO)
            .unwrap_or_else(|error| panic!("drive shutdown during backoff: {error}"));
        if matches!(outcome, TurnOutcome::Shutdown { .. }) {
            assert!(reactor.is_shutdown());
            return;
        }
    }
    panic!("shutdown during direct backoff did not become terminal");
}
