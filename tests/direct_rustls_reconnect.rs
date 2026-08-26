//! Public numeric rustls reconnection through one long-lived Bornera owner.

#![cfg(feature = "tls-rustls")]

#[path = "support/sasl_broker.rs"]
mod sasl_broker;
#[path = "support/scram.rs"]
mod scram;
#[path = "support/tls_broker.rs"]
mod tls_broker;

use std::{
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use kafka_driver::{
    BrokerState, Call, ConnectionCloseReason, ConnectionPhase, Driver, DriverSnapshot,
    NegotiationFailure, Reactor, SaslConfig, TlsClientConfig,
};
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};
use kafka_wire_core::Bytes;
use rustls::{ServerConfig, ServerConnection, StreamOwned};

use sasl_broker::{AuthenticationReply, HandshakeReply, SaslPeer};
use scram::{ScramAlgorithm, ScramTranscript};
use tls_broker::TlsBroker;

const USERNAME: &str = "user";
const PASSWORD: &str = "pencil";

type TlsStream = StreamOwned<ServerConnection, TcpStream>;

#[test]
fn queued_call_completes_on_rustls_epoch_two() {
    assert_generation_two(None, |peer| {
        let negotiation = peer.expect_negotiation();
        assert_eq!(negotiation, 0);
        peer.respond_to_negotiation(negotiation);
        let public = peer.expect_generated_call();
        assert_eq!(public, 1);
        peer.respond_to_generated_call(public);
    });
}

#[test]
fn queued_call_completes_after_plain_authentication_on_rustls_epoch_two() {
    let sasl = SaslConfig::plain("alice", "s3cret")
        .unwrap_or_else(|error| panic!("construct reconnecting rustls PLAIN config: {error}"))
        .with_authorization_identity("service")
        .unwrap_or_else(|error| panic!("construct reconnecting rustls PLAIN authzid: {error}"));
    assert_generation_two(Some(sasl), |peer| {
        let negotiation = peer.expect_negotiation();
        assert_eq!(negotiation, 0);
        peer.respond_to_negotiation(negotiation);
        let handshake = peer.expect_plain_handshake();
        assert_eq!(handshake, 1);
        peer.respond_to_handshake(handshake, HandshakeReply::Accepted);
        let authentication = peer.expect_plain_authentication(b"service\0alice\0s3cret");
        assert_eq!(authentication, 2);
        peer.respond_to_authentication(authentication, AuthenticationReply::Accepted);
        let public = peer.expect_generated_call();
        assert_eq!(public, 3);
        peer.respond_to_generated_call(public);
    });
}

#[test]
fn queued_call_completes_after_scram_authentication_on_rustls_epoch_two() {
    let algorithm = ScramAlgorithm::Sha256;
    assert_generation_two(
        Some(algorithm.configuration(USERNAME, PASSWORD)),
        move |peer| {
            let negotiation = peer.expect_negotiation();
            assert_eq!(negotiation, 0);
            peer.respond_to_negotiation(negotiation);
            let handshake = peer.expect_handshake(algorithm.mechanism());
            assert_eq!(handshake, 1);
            peer.respond_to_handshake_for(
                handshake,
                algorithm.mechanism(),
                HandshakeReply::Accepted,
            );
            let first = peer.expect_authentication();
            assert_eq!(first.correlation, 2);
            let transcript = ScramTranscript::from_client_first(
                algorithm,
                &first.auth_bytes,
                USERNAME,
                PASSWORD,
            );
            peer.respond_to_authentication_with(
                first.correlation,
                AuthenticationReply::Accepted,
                Bytes::copy_from_slice(transcript.server_first()),
            );
            let final_request = peer.expect_authentication();
            assert_eq!(final_request.correlation, 3);
            assert_eq!(final_request.auth_bytes.as_ref(), transcript.client_final());
            peer.respond_to_authentication_with(
                final_request.correlation,
                AuthenticationReply::Accepted,
                Bytes::copy_from_slice(transcript.server_final()),
            );
            let public = peer.expect_generated_call();
            assert_eq!(public, 4);
            peer.respond_to_generated_call(public);
        },
    );
}

fn assert_generation_two<F>(sasl: Option<SaslConfig>, second_generation: F)
where
    F: FnOnce(&mut SaslPeer<TlsStream>) + Send + 'static,
{
    let (address, tls, first_dropped, release, owner) =
        spawn_two_generation_broker(second_generation);
    let builder = Driver::builder().rustls_broker(address, tls);
    let builder = match sasl {
        Some(sasl) => builder.sasl(sasl),
        None => builder,
    };
    let (driver, mut reactor) = builder
        .build_reactor()
        .unwrap_or_else(|error| panic!("build reconnecting rustls reactor: {error}"));
    let call = driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("admit call before rustls readiness: {error}"));

    assert_eq!(
        drive_until_first_drop(&mut reactor, &first_dropped),
        0,
        "generation one must begin with negotiation correlation zero",
    );
    let first_close = ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Malformed);
    assert!(
        call.try_result().is_none(),
        "queued call must survive the first-generation loss"
    );

    assert_eq!(
        drive_call(&mut reactor, &call),
        Ok(Ok(ApiVersionsResponse::default()))
    );
    let snapshot = current_snapshot(&driver, &mut reactor);
    let seed = snapshot
        .seed()
        .unwrap_or_else(|| panic!("reconnected rustls seed must remain observable"));
    assert!(matches!(
        seed.broker_state(),
        BrokerState::Available { epoch } if epoch.get() == 2
    ));
    assert_eq!(seed.connection_phase(), ConnectionPhase::Ready);
    assert_eq!(seed.last_close_reason(), Some(first_close));
    assert_eq!(snapshot.calls().admitted(), 1);
    assert_eq!(snapshot.calls().succeeded(), 1);
    assert_eq!(snapshot.calls().failed(), 0);

    release
        .send(())
        .unwrap_or_else(|error| panic!("release rustls generation two: {error}"));
    owner
        .join()
        .unwrap_or_else(|_| panic!("reconnecting rustls broker must finish cleanly"));
}

fn spawn_two_generation_broker<F>(
    second_generation: F,
) -> (
    SocketAddr,
    TlsClientConfig,
    mpsc::Receiver<i32>,
    mpsc::SyncSender<()>,
    thread::JoinHandle<()>,
)
where
    F: FnOnce(&mut SaslPeer<TlsStream>) + Send + 'static,
{
    let broker = TlsBroker::bind();
    let address = broker.address();
    let tls = broker.client_config();
    let (listener, server) = broker.into_server_parts();
    let (dropped, first_dropped) = mpsc::sync_channel(1);
    let (release, released) = mpsc::sync_channel(1);
    let owner = thread::spawn(move || {
        let mut first = accept_peer(&listener, &server);
        let correlation = first.expect_negotiation();
        drop(first);
        dropped
            .send(correlation)
            .unwrap_or_else(|error| panic!("report first rustls drop: {error}"));

        let mut second = accept_peer(&listener, &server);
        second_generation(&mut second);
        released
            .recv()
            .unwrap_or_else(|error| panic!("hold rustls generation two: {error}"));
    });
    (address, tls, first_dropped, release, owner)
}

fn accept_peer(listener: &TcpListener, server: &Arc<ServerConfig>) -> SaslPeer<TlsStream> {
    let (tcp, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept rustls reconnect generation: {error}"));
    tcp.set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap_or_else(|error| panic!("bound rustls reconnect read: {error}"));
    tcp.set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap_or_else(|error| panic!("bound rustls reconnect write: {error}"));
    let connection = ServerConnection::new(Arc::clone(server))
        .unwrap_or_else(|error| panic!("construct rustls reconnect server: {error}"));
    SaslPeer::new(StreamOwned::new(connection, tcp))
}

fn drive_until_first_drop(reactor: &mut Reactor, dropped: &mpsc::Receiver<i32>) -> i32 {
    for _ in 0..64 {
        if let Ok(correlation) = dropped.try_recv() {
            return correlation;
        }
        drive_once(reactor);
    }
    panic!("rustls generation one did not publish its negotiation request");
}

fn current_snapshot(driver: &Driver, reactor: &mut Reactor) -> DriverSnapshot {
    let snapshot = driver
        .snapshot()
        .unwrap_or_else(|error| panic!("admit rustls reconnect snapshot: {error}"));
    reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("interpret rustls reconnect snapshot: {error}"));
    snapshot
        .wait()
        .unwrap_or_else(|error| panic!("receive rustls reconnect snapshot: {error}"))
        .unwrap_or_else(|error| panic!("build rustls reconnect snapshot: {error}"))
}

fn drive_call<T>(
    reactor: &mut Reactor,
    call: &Call<T>,
) -> Result<T, kafka_driver::CompletionError> {
    for _ in 0..128 {
        if let Some(result) = call.try_result() {
            return result;
        }
        drive_once(reactor);
    }
    panic!("reconnected rustls call remained pending");
}

fn drive_once(reactor: &mut Reactor) {
    reactor
        .turn(Duration::from_millis(50))
        .unwrap_or_else(|error| panic!("drive rustls reconnect turn: {error}"));
}
