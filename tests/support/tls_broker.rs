//! Blocking rustls broker fixture isolated from the nonblocking driver owner.

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver::{ApiVersion, TlsClientConfig, TlsClientPolicy};
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
const LOOPBACK_IP_CERTIFICATE: &[u8] = include_bytes!("../fixtures/tls/loopback-ip-cert.pem");
const LOOPBACK_IP_PRIVATE_KEY: &[u8] = include_bytes!("../fixtures/tls/loopback-ip-key.pem");

#[allow(
    dead_code,
    reason = "fixture steps are selected by separate TLS integration scenarios"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BrokerStep {
    NegotiationRejected,
    NegotiationResponded,
    CallResponded,
    CallRespondedBeforeMalformedFrame,
}

#[allow(
    dead_code,
    reason = "fixture identities are selected by separate TLS integration scenarios"
)]
#[derive(Clone, Copy)]
enum TlsIdentity {
    Localhost,
    LoopbackIp,
}

pub(crate) struct TlsBroker {
    listener: TcpListener,
    client: Arc<ClientConfig>,
    server: Arc<ServerConfig>,
}

impl TlsBroker {
    pub(crate) fn bind() -> Self {
        Self::bind_with(TlsIdentity::Localhost)
    }

    #[allow(
        dead_code,
        reason = "shared fixture method is selected by the bootstrap-rotation TLS scenario"
    )]
    pub(crate) fn bind_loopback_ip() -> Self {
        Self::bind_with(TlsIdentity::LoopbackIp)
    }

    fn bind_with(identity: TlsIdentity) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|error| panic!("bind TLS loopback broker: {error}"));
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = RootCertStore::empty();
        for certificate in [certificate(), loopback_ip_certificate()] {
            roots
                .add(certificate)
                .unwrap_or_else(|error| panic!("trust TLS test certificate: {error}"));
        }
        let client = ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .unwrap_or_else(|error| panic!("select TLS client versions: {error}"))
            .with_root_certificates(roots)
            .with_no_client_auth();
        let server = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap_or_else(|error| panic!("select TLS server versions: {error}"))
            .with_no_client_auth()
            .with_single_cert(vec![identity.certificate()], identity.private_key())
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

    #[allow(
        dead_code,
        reason = "shared fixture method is selected by the direct-TLS integration scenario"
    )]
    pub(crate) fn client_config(&self) -> TlsClientConfig {
        let server_name = ServerName::try_from("localhost")
            .unwrap_or_else(|error| panic!("construct TLS server name: {error}"));
        self.client_policy().for_server(server_name)
    }

    #[allow(
        dead_code,
        reason = "shared fixture method is selected by the bootstrap-TLS integration scenario"
    )]
    pub(crate) fn client_policy(&self) -> TlsClientPolicy {
        TlsClientPolicy::new(Arc::clone(&self.client))
    }

    pub(crate) fn spawn(self) -> (mpsc::Receiver<BrokerStep>, thread::JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel();
        let owner = thread::spawn(move || self.serve(&sender));
        (receiver, owner)
    }

    #[allow(dead_code, reason = "used only by the TLS read-failure test")]
    pub(crate) fn spawn_malformed_after_call(
        self,
    ) -> (mpsc::Receiver<BrokerStep>, thread::JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel();
        let owner = thread::spawn(move || self.serve_malformed_after_call(&sender));
        (receiver, owner)
    }

    #[allow(
        dead_code,
        reason = "shared fixture method is selected by the bootstrap-rotation TLS scenario"
    )]
    pub(crate) fn spawn_rejecting_negotiation(
        self,
    ) -> (mpsc::Receiver<BrokerStep>, thread::JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel();
        let owner = thread::spawn(move || self.reject_negotiation(&sender));
        (receiver, owner)
    }

    fn serve(self, steps: &mpsc::Sender<BrokerStep>) {
        let mut stream = self.accept_stream();

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

    #[allow(dead_code, reason = "used only by the TLS read-failure test")]
    fn serve_malformed_after_call(self, steps: &mpsc::Sender<BrokerStep>) {
        let mut stream = self.accept_stream();
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
        let mut response = call_response();
        response.extend_from_slice(&(-1_i32).to_be_bytes());
        stream
            .write_all(&response)
            .unwrap_or_else(|error| panic!("write TLS response and malformed frame: {error}"));
        stream
            .flush()
            .unwrap_or_else(|error| panic!("flush TLS response and malformed frame: {error}"));
        steps
            .send(BrokerStep::CallRespondedBeforeMalformedFrame)
            .unwrap_or_else(|error| panic!("report TLS malformed response batch: {error}"));
    }

    #[allow(
        dead_code,
        reason = "shared fixture branch is selected by the bootstrap-rotation TLS scenario"
    )]
    fn reject_negotiation(self, steps: &mpsc::Sender<BrokerStep>) {
        let mut stream = self.accept_stream();
        read_frame(&mut stream);
        stream
            .write_all(&0_i32.to_be_bytes())
            .unwrap_or_else(|error| panic!("write rejected TLS negotiation: {error}"));
        stream
            .flush()
            .unwrap_or_else(|error| panic!("flush rejected TLS negotiation: {error}"));
        steps
            .send(BrokerStep::NegotiationRejected)
            .unwrap_or_else(|error| panic!("report rejected TLS negotiation: {error}"));
    }

    fn accept_stream(self) -> StreamOwned<ServerConnection, TcpStream> {
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
        StreamOwned::new(session, socket)
    }
}

impl TlsIdentity {
    fn certificate(self) -> CertificateDer<'static> {
        match self {
            Self::Localhost => certificate(),
            Self::LoopbackIp => loopback_ip_certificate(),
        }
    }

    fn private_key(self) -> PrivateKeyDer<'static> {
        match self {
            Self::Localhost => private_key(),
            Self::LoopbackIp => loopback_ip_private_key(),
        }
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

fn loopback_ip_certificate() -> CertificateDer<'static> {
    CertificateDer::from_pem_slice(LOOPBACK_IP_CERTIFICATE)
        .unwrap_or_else(|error| panic!("parse loopback-IP TLS test certificate: {error}"))
}

fn loopback_ip_private_key() -> PrivateKeyDer<'static> {
    PrivateKeyDer::from_pem_slice(LOOPBACK_IP_PRIVATE_KEY)
        .unwrap_or_else(|error| panic!("parse loopback-IP TLS test private key: {error}"))
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
