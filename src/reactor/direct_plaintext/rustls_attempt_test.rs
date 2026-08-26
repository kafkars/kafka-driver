//! Same-set Rustls attempt replay with a fresh transport and decoder gate.

use std::{io, net::TcpListener, sync::Arc};

use bornera::{ConnectError, OwnerFailure};
use bornera_core::ConnectionEpoch;
use calandria::RetainedBytes;
use kafka_driver_core::Moment;
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};

use crate::{DriverLimits, TlsClientConfig};

use super::{
    attempt::{DirectConnectError, rustls_connect_error},
    runtime::DirectRuntime,
    rustls_transport::DirectRustlsTransport,
};

#[test]
fn rustls_connect_classifies_only_raw_socket_io_as_endpoint_local() {
    let endpoint = rustls_connect_error(ConnectError::<io::Error>::Io(
        io::ErrorKind::ConnectionRefused.into(),
    ));
    assert!(matches!(endpoint, DirectConnectError::Endpoint(_)));

    let adapter = bornera_rustls::RustlsConnectError::Capacity {
        required: RetainedBytes::from(2_u32),
        supplied: RetainedBytes::from(1_u32),
    };
    let wrapped = io::Error::new(io::ErrorKind::InvalidInput, adapter);
    let adapter = rustls_connect_error(ConnectError::<io::Error>::Io(wrapped));
    assert!(matches!(adapter, DirectConnectError::Fatal(_)));

    let selector = rustls_connect_error(ConnectError::<io::Error>::ResourceAdmission);
    assert!(matches!(selector, DirectConnectError::Fatal(_)));
}

#[test]
fn rustls_attempt_replays_a_fresh_transport_in_one_set() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind Rustls replay listener: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read Rustls replay address: {error}"));
    let mut owner = DirectRuntime::<DirectRustlsTransport>::new(
        &DriverLimits::default(),
        address,
        tls(),
        None,
        None,
        Moment::from_nanos(1),
    )
    .unwrap_or_else(|error| panic!("construct first Rustls generation: {error}"));
    let first = owner
        .live_connection()
        .unwrap_or_else(|error| panic!("read first rustls generation: {error}"));

    let report = owner
        .set
        .abandon(first, OwnerFailure::OwnerInvariant)
        .unwrap_or_else(|error| panic!("recover first Rustls generation: {error}"));
    assert_eq!(report.epoch, ConnectionEpoch::new(1));
    let second = owner
        .lane
        .connection_attempt
        .connect(
            &mut owner.set,
            owner.lane.connection_owner,
            ConnectionEpoch::new(2),
            Moment::from_nanos(2),
        )
        .unwrap_or_else(|error| panic!("construct second Rustls generation: {error}"));

    assert_ne!(second, first);
    assert_eq!(second.connection(), first.connection());
    assert_eq!(second.epoch(), ConnectionEpoch::new(2));
    assert_eq!(owner.set.snapshot().connections.active(), 1);
    assert_eq!(owner.set.snapshot().poller.registrations(), 1);
}

fn tls() -> TlsClientConfig {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap_or_else(|error| panic!("select Rustls protocol versions: {error}"))
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    let server_name = ServerName::try_from("replay.test".to_owned())
        .unwrap_or_else(|error| panic!("construct Rustls replay name: {error}"));
    TlsClientConfig::new(Arc::new(client), server_name)
}
