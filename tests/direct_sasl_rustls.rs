//! Public configured numeric rustls SASL PLAIN sentinel through the direct Bornera owner.

#![cfg(feature = "tls-rustls")]

#[path = "support/sasl_broker.rs"]
mod sasl_broker;
#[path = "support/tls_broker.rs"]
mod tls_broker;

use std::{thread, time::Duration};

use kafka_driver::{Call, Driver, Reactor, SaslConfig};
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};
use rustls::{ServerConnection, StreamOwned};

use sasl_broker::{AuthenticationReply, HandshakeReply, SaslPeer};
use tls_broker::TlsBroker;

#[test]
fn configured_numeric_rustls_plain_authenticates_before_the_generated_call() {
    let broker = TlsBroker::bind();
    let address = broker.address();
    let tls = broker.client_config();
    let (listener, server) = broker.into_server_parts();
    let owner = thread::spawn(move || {
        let (tcp, _) = listener
            .accept()
            .unwrap_or_else(|error| panic!("accept rustls SASL connection: {error}"));
        tcp.set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap_or_else(|error| panic!("bound rustls SASL read: {error}"));
        let connection = ServerConnection::new(server)
            .unwrap_or_else(|error| panic!("construct rustls SASL server: {error}"));
        let mut peer = SaslPeer::new(StreamOwned::new(connection, tcp));

        let negotiation = peer.expect_negotiation();
        peer.respond_to_negotiation(negotiation);
        let handshake = peer.expect_plain_handshake();
        peer.respond_to_handshake(handshake, HandshakeReply::Accepted);
        let authentication = peer.expect_plain_authentication(b"service\0alice\0s3cret");
        peer.respond_to_authentication(authentication, AuthenticationReply::Accepted);
        let call = peer.expect_generated_call();
        peer.respond_to_generated_call(call);
    });
    let sasl = SaslConfig::plain("alice", "s3cret")
        .unwrap_or_else(|error| panic!("construct rustls PLAIN credentials: {error}"))
        .with_authorization_identity("service")
        .unwrap_or_else(|error| panic!("construct rustls PLAIN authzid: {error}"));
    let (driver, mut reactor) = Driver::builder()
        .rustls_broker(address, tls)
        .sasl(sasl)
        .build_reactor()
        .unwrap_or_else(|error| panic!("build configured rustls PLAIN reactor: {error}"));
    let call = driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("admit call behind rustls PLAIN: {error}"));

    let result = drive_call(&mut reactor, &call);
    owner
        .join()
        .unwrap_or_else(|_| panic!("rustls PLAIN broker must finish cleanly"));

    assert_eq!(result, Ok(Ok(ApiVersionsResponse::default())));
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
            .turn(Duration::from_millis(100))
            .unwrap_or_else(|error| panic!("drive rustls PLAIN call: {error}"));
    }
    panic!("rustls PLAIN call remained pending");
}
