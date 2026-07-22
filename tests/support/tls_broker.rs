//! Blocking rustls broker fixture isolated from the nonblocking driver owner.

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver::{ApiVersion, TlsClientConfig};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi, response_header_version_for,
};
use kafka_wire_core::KafkaEncode;
use rustls::{
    ClientConfig, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
};

const CERTIFICATE: &[u8] = include_bytes!("../fixtures/tls/localhost-cert.pem");
const PRIVATE_KEY: &[u8] = include_bytes!("../fixtures/tls/localhost-key.pem");

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BrokerStep {
    NegotiationResponded,
    CallResponded,
}

pub(crate) struct TlsBroker {
    listener: TcpListener,
    client: Arc<ClientConfig>,
    server: Arc<ServerConfig>,
}

impl TlsBroker {
    pub(crate) fn bind() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|error| panic!("bind TLS loopback broker: {error}"));
        let certificate = certificate();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = RootCertStore::empty();
        roots
            .add(certificate.clone())
            .unwrap_or_else(|error| panic!("trust TLS test certificate: {error}"));
        let client = ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .unwrap_or_else(|error| panic!("select TLS client versions: {error}"))
            .with_root_certificates(roots)
            .with_no_client_auth();
        let server = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap_or_else(|error| panic!("select TLS server versions: {error}"))
            .with_no_client_auth()
            .with_single_cert(vec![certificate], private_key())
            .unwrap_or_else(|error| panic!("configure TLS test identity: {error}"));
        Self {
            listener,
            client: Arc::new(client),
            server: Arc::new(server),
        }
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .unwrap_or_else(|error| panic!("read TLS loopback address: {error}"))
    }

    pub(crate) fn client_config(&self) -> TlsClientConfig {
        let server_name = ServerName::try_from("localhost")
            .unwrap_or_else(|error| panic!("construct TLS server name: {error}"));
        TlsClientConfig::new(Arc::clone(&self.client), server_name)
    }

    pub(crate) fn spawn(self) -> (mpsc::Receiver<BrokerStep>, thread::JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel();
        let owner = thread::spawn(move || self.serve(&sender));
        (receiver, owner)
    }

    fn serve(self, steps: &mpsc::Sender<BrokerStep>) {
        let (socket, _) = self
            .listener
            .accept()
            .unwrap_or_else(|error| panic!("accept TLS driver connection: {error}"));
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap_or_else(|error| panic!("bound TLS broker read: {error}"));
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap_or_else(|error| panic!("bound TLS broker write: {error}"));
        let session = ServerConnection::new(self.server)
            .unwrap_or_else(|error| panic!("start TLS server session: {error}"));
        let mut stream = StreamOwned::new(session, socket);

        read_frame(&mut stream);
        stream
            .write_all(&negotiation_response())
            .unwrap_or_else(|error| panic!("write TLS negotiation response: {error}"));
        stream
            .flush()
            .unwrap_or_else(|error| panic!("flush TLS negotiation response: {error}"));
        steps
            .send(BrokerStep::NegotiationResponded)
            .unwrap_or_else(|error| panic!("report TLS negotiation response: {error}"));

        read_frame(&mut stream);
        stream
            .write_all(&call_response())
            .unwrap_or_else(|error| panic!("write TLS call response: {error}"));
        stream
            .flush()
            .unwrap_or_else(|error| panic!("flush TLS call response: {error}"));
        steps
            .send(BrokerStep::CallResponded)
            .unwrap_or_else(|error| panic!("report TLS call response: {error}"));
    }
}

fn certificate() -> CertificateDer<'static> {
    CertificateDer::from_pem_slice(CERTIFICATE)
        .unwrap_or_else(|error| panic!("parse TLS test certificate: {error}"))
}

fn private_key() -> PrivateKeyDer<'static> {
    PrivateKeyDer::from_pem_slice(PRIVATE_KEY)
        .unwrap_or_else(|error| panic!("parse TLS test private key: {error}"))
}

fn read_frame(stream: &mut impl Read) {
    let mut prefix = [0; size_of::<i32>()];
    stream
        .read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read TLS Kafka frame length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("validate TLS Kafka frame length: {error}"));
    let mut body = vec![0; length];
    stream
        .read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read TLS Kafka frame body: {error}"));
    assert!(!body.is_empty(), "TLS Kafka frame body must not be empty");
}

fn negotiation_response() -> Vec<u8> {
    let mut response = ApiVersionsResponse::default();
    let mut api = AdvertisedApi::default();
    api.api_key = API_VERSIONS_API_DESCRIPTOR.api_key.value();
    api.min_version = 0;
    api.max_version = 0;
    response.api_keys.push(api);
    encoded_response(&response, ApiVersion::new(0))
}

fn call_response() -> Vec<u8> {
    encoded_response(&ApiVersionsResponse::default(), ApiVersion::new(0))
}

fn encoded_response(response: &ApiVersionsResponse, version: ApiVersion) -> Vec<u8> {
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = 0;
    let header_version = response_header_version_for::<ApiVersionsRequest>(version)
        .unwrap_or_else(|error| panic!("select TLS response header version: {error}"));
    assert!(
        header
            .encode_into(&mut body, ApiVersion::new(header_version))
            .is_ok()
    );
    assert!(response.encode_into(&mut body, version).is_ok());
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("bound TLS Kafka response length: {error}"));
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}
