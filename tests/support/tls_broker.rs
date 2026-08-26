//! Blocking rustls broker fixture isolated from the nonblocking driver owner.

#![allow(
    dead_code,
    reason = "shared TLS fixture methods are selected by separate integration targets"
)]

#[path = "tls_broker/codec.rs"]
mod codec;
#[path = "tls_broker/crypto.rs"]
mod crypto;
#[path = "tls_broker/server.rs"]
mod server;

use std::{
    net::{SocketAddr, TcpListener},
    sync::{Arc, mpsc},
    thread,
};

use kafka_driver::{TlsClientConfig, TlsClientPolicy};
use rustls::{ClientConfig, ServerConfig, pki_types::ServerName};

use crypto::TlsIdentity;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BrokerStep {
    NegotiationRejected,
    NegotiationResponded,
    NegotiationRespondedWithoutSni,
    CallResponded,
    CallRespondedBeforeMalformedFrame,
    CallsRespondedBeforeTruncation,
    CallReadBeforeTruncation,
    TlsHandshakeRejectedBeforeKafka,
    CloseNotifyObserved,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TerminalScript {
    CloseNotifyAfterOne,
    TruncateAfterOne,
    TruncateAfterTwo,
    PartialAfterOne,
}

impl TerminalScript {
    pub(crate) const fn generation_one_calls(self) -> usize {
        match self {
            Self::TruncateAfterTwo => 3,
            Self::CloseNotifyAfterOne | Self::TruncateAfterOne | Self::PartialAfterOne => 2,
        }
    }

    pub(crate) const fn complete_responses(self) -> usize {
        match self {
            Self::TruncateAfterTwo => 2,
            Self::CloseNotifyAfterOne | Self::TruncateAfterOne | Self::PartialAfterOne => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TerminalStep {
    GenerationOneClosed,
    GenerationTwoNegotiated,
    ProbeResponded,
}

pub(crate) struct TlsBroker {
    pub(super) listener: TcpListener,
    client: Arc<ClientConfig>,
    pub(super) server: Arc<ServerConfig>,
}

impl TlsBroker {
    pub(crate) fn bind() -> Self {
        Self::bind_with(TlsIdentity::Localhost)
    }

    pub(crate) fn bind_loopback_ip() -> Self {
        Self::bind_with(TlsIdentity::LoopbackIp)
    }

    fn bind_with(identity: TlsIdentity) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|error| panic!("bind TLS loopback broker: {error}"));
        let (client, server) = crypto::configs(identity);
        Self {
            listener,
            client,
            server,
        }
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .unwrap_or_else(|error| panic!("read TLS loopback address: {error}"))
    }

    pub(crate) fn client_config(&self) -> TlsClientConfig {
        self.client_config_for("localhost")
    }

    pub(crate) fn client_config_for_ip(&self) -> TlsClientConfig {
        self.client_config_for("127.0.0.1")
    }

    pub(crate) fn client_config_for(&self, identity: &str) -> TlsClientConfig {
        let server_name = ServerName::try_from(identity.to_owned())
            .unwrap_or_else(|error| panic!("construct TLS server name: {error}"));
        self.client_policy().for_server(server_name)
    }

    pub(crate) fn client_policy(&self) -> TlsClientPolicy {
        TlsClientPolicy::new(Arc::clone(&self.client))
    }

    pub(crate) fn into_server_parts(self) -> (TcpListener, Arc<ServerConfig>) {
        (self.listener, self.server)
    }

    pub(crate) fn spawn(self) -> (mpsc::Receiver<BrokerStep>, thread::JoinHandle<()>) {
        self.spawn_with(Self::serve)
    }

    pub(crate) fn spawn_expecting_no_sni(
        self,
    ) -> (mpsc::Receiver<BrokerStep>, thread::JoinHandle<()>) {
        self.spawn_with(Self::serve_expecting_no_sni)
    }

    pub(crate) fn spawn_malformed_after_call(
        self,
    ) -> (mpsc::Receiver<BrokerStep>, thread::JoinHandle<()>) {
        self.spawn_with(Self::serve_malformed_after_call)
    }

    pub(crate) fn spawn_truncating_after_call(
        self,
    ) -> (mpsc::Receiver<BrokerStep>, thread::JoinHandle<()>) {
        self.spawn_with(Self::serve_truncating_after_call)
    }

    pub(crate) fn spawn_two_calls_before_truncation(
        self,
    ) -> (mpsc::Receiver<BrokerStep>, thread::JoinHandle<()>) {
        self.spawn_with(Self::serve_two_calls_before_truncation)
    }

    pub(crate) fn spawn_terminal_ordering(
        self,
        script: TerminalScript,
    ) -> (
        mpsc::Receiver<TerminalStep>,
        mpsc::SyncSender<()>,
        thread::JoinHandle<()>,
    ) {
        let (sender, receiver) = mpsc::channel();
        let (release, released) = mpsc::sync_channel(1);
        let owner = thread::spawn(move || self.serve_terminal_ordering(script, &sender, &released));
        (receiver, release, owner)
    }

    pub(crate) fn spawn_observing_identity_rejection(
        self,
    ) -> (mpsc::Receiver<BrokerStep>, thread::JoinHandle<()>) {
        self.spawn_with(Self::observe_identity_rejection)
    }

    pub(crate) fn spawn_observing_close_notify(
        self,
    ) -> (mpsc::Receiver<BrokerStep>, thread::JoinHandle<()>) {
        self.spawn_with(Self::serve_observing_close_notify)
    }

    pub(crate) fn spawn_rejecting_negotiation(
        self,
    ) -> (mpsc::Receiver<BrokerStep>, thread::JoinHandle<()>) {
        self.spawn_with(Self::reject_negotiation)
    }

    fn spawn_with(
        self,
        serve: fn(Self, &mpsc::Sender<BrokerStep>),
    ) -> (mpsc::Receiver<BrokerStep>, thread::JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel();
        let owner = thread::spawn(move || serve(self, &sender));
        (receiver, owner)
    }
}
