//! Public configured numeric rustls SCRAM matrix through the direct Bornera owner.

#![cfg(feature = "tls-rustls")]

#[path = "support/sasl_broker.rs"]
mod sasl_broker;
#[path = "support/scram.rs"]
mod scram;
#[path = "support/tls_broker.rs"]
mod tls_broker;

use std::{
    io::{ErrorKind, Read},
    net::TcpStream,
    sync::mpsc,
    thread,
    time::Duration,
};

use kafka_driver::{Call, Driver, Reactor, RequestError};
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};
use kafka_wire_core::Bytes;
use rustls::{ServerConnection, StreamOwned};

use sasl_broker::{AuthenticationReply, HandshakeReply, SaslPeer};
use scram::{ScramAlgorithm, ScramTranscript};
use tls_broker::TlsBroker;

const USERNAME: &str = "user";
const PASSWORD: &str = "pencil";

#[test]
fn configured_numeric_rustls_scram_sha_256_verifies_before_readiness() {
    assert_success(ScramAlgorithm::Sha256);
}

#[test]
fn configured_numeric_rustls_scram_sha_512_verifies_before_readiness() {
    assert_success(ScramAlgorithm::Sha512);
}

fn assert_success(algorithm: ScramAlgorithm) {
    let broker = TlsBroker::bind();
    let address = broker.address();
    let tls = broker.client_config();
    let (listener, server) = broker.into_server_parts();
    let (verified, observed) = mpsc::sync_channel(1);
    let (advance, readiness_steps) = mpsc::sync_channel(1);
    let (quiet, quiet_observed) = mpsc::sync_channel(1);
    let broker_owner = thread::spawn(move || {
        let (tcp, _) = listener
            .accept()
            .unwrap_or_else(|error| panic!("accept rustls SCRAM connection: {error}"));
        tcp.set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap_or_else(|error| panic!("bound rustls SCRAM read: {error}"));
        let connection = ServerConnection::new(server)
            .unwrap_or_else(|error| panic!("construct rustls SCRAM server: {error}"));
        let mut peer = SaslPeer::new(StreamOwned::new(connection, tcp));

        let negotiation = peer.expect_negotiation();
        assert_eq!(negotiation, 0);
        peer.respond_to_negotiation(negotiation);
        let handshake = peer.expect_handshake(algorithm.mechanism());
        assert_eq!(handshake, 1);
        peer.respond_to_handshake_for(handshake, algorithm.mechanism(), HandshakeReply::Accepted);
        let first = peer.expect_authentication();
        assert_eq!(first.correlation, 2);
        let transcript =
            ScramTranscript::from_client_first(algorithm, &first.auth_bytes, USERNAME, PASSWORD);
        peer.respond_to_authentication_with(
            first.correlation,
            AuthenticationReply::Accepted,
            Bytes::copy_from_slice(transcript.server_first()),
        );
        let final_request = peer.expect_authentication();
        assert_eq!(final_request.correlation, 3);
        assert_eq!(final_request.auth_bytes.as_ref(), transcript.client_final());
        verified
            .send(())
            .unwrap_or_else(|error| panic!("publish verified rustls SCRAM proof: {error}"));
        readiness_steps
            .recv()
            .unwrap_or_else(|error| panic!("await rustls SCRAM readiness probe: {error}"));
        assert_no_decrypted_request(&mut peer);
        quiet
            .send(())
            .unwrap_or_else(|error| panic!("publish quiet rustls SCRAM stream: {error}"));
        readiness_steps
            .recv()
            .unwrap_or_else(|error| panic!("release rustls SCRAM server final: {error}"));
        peer.respond_to_authentication_with(
            final_request.correlation,
            AuthenticationReply::Accepted,
            Bytes::copy_from_slice(transcript.server_final()),
        );
        let public = peer.expect_generated_call();
        assert_eq!(public, 4);
        peer.respond_to_generated_call(public);
    });
    let (driver, mut reactor) = Driver::builder()
        .rustls_broker(address, tls)
        .sasl(algorithm.configuration(USERNAME, PASSWORD))
        .build_reactor()
        .unwrap_or_else(|error| panic!("build rustls {} reactor: {error}", algorithm.mechanism()));
    let call = driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("admit call behind rustls SCRAM: {error}"));

    drive_until_verified(&mut reactor, &call, &observed);
    drive_while_server_final_is_withheld(&mut reactor, &call);
    advance
        .send(())
        .unwrap_or_else(|error| panic!("request rustls SCRAM readiness probe: {error}"));
    quiet_observed
        .recv_timeout(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("observe quiet rustls SCRAM stream: {error}"));
    advance
        .send(())
        .unwrap_or_else(|error| panic!("release rustls SCRAM completion: {error}"));
    let result = drive_call(&mut reactor, &call);
    broker_owner
        .join()
        .unwrap_or_else(|_| panic!("rustls SCRAM broker must finish cleanly"));
    assert_eq!(result, Ok(Ok(ApiVersionsResponse::default())));
}

fn drive_until_verified(
    reactor: &mut Reactor,
    call: &Call<Result<ApiVersionsResponse, RequestError>>,
    observed: &mpsc::Receiver<()>,
) {
    for _ in 0..96 {
        if observed.try_recv().is_ok() {
            return;
        }
        assert!(call.try_result().is_none());
        reactor
            .turn(Duration::from_millis(50))
            .unwrap_or_else(|error| panic!("drive rustls SCRAM proof: {error}"));
    }
    panic!("rustls SCRAM client final was not verified within bounded turns");
}

fn drive_while_server_final_is_withheld(
    reactor: &mut Reactor,
    call: &Call<Result<ApiVersionsResponse, RequestError>>,
) {
    for _ in 0..2 {
        assert!(call.try_result().is_none());
        reactor
            .turn(Duration::from_millis(25))
            .unwrap_or_else(|error| panic!("drive withheld rustls SCRAM final: {error}"));
    }
}

fn assert_no_decrypted_request(peer: &mut SaslPeer<StreamOwned<ServerConnection, TcpStream>>) {
    let stream = peer.stream_mut();
    stream
        .sock
        .set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make rustls SCRAM stream nonblocking: {error}"));
    let mut probe = [0; 1];
    let observed = stream.read(&mut probe);
    stream
        .sock
        .set_nonblocking(false)
        .unwrap_or_else(|error| panic!("restore rustls SCRAM stream blocking mode: {error}"));
    match observed {
        Err(error) if error.kind() == ErrorKind::WouldBlock => {}
        Ok(0) => panic!("rustls SCRAM stream closed before server-final"),
        Ok(_) => panic!("public request escaped before rustls SCRAM server-final"),
        Err(error) => panic!("probe decrypted rustls SCRAM stream: {error}"),
    }
}

fn drive_call<T>(
    reactor: &mut Reactor,
    call: &Call<T>,
) -> Result<T, kafka_driver::CompletionError> {
    for _ in 0..96 {
        if let Some(result) = call.try_result() {
            return result;
        }
        reactor
            .turn(Duration::from_millis(50))
            .unwrap_or_else(|error| panic!("drive rustls SCRAM call: {error}"));
    }
    panic!("rustls SCRAM call remained pending");
}
