//! Authentication-terminal scenarios for calls waiting behind broker startup.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    num::{NonZeroU16, NonZeroUsize},
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver_core::{
    AuthenticationFailure, BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits,
    BrokerEndpoint, BrokerId, CallFailure, CloseReason, Delivery, DnsOutcome, EffectId, HostName,
    IpAddress, MetadataGeneration, Moment, ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, ResponseHeader,
    SASL_AUTHENTICATE_API_DESCRIPTOR, SASL_HANDSHAKE_API_DESCRIPTOR, SaslHandshakeResponse,
    api_versions_response::ApiVersion as AdvertisedApi,
};
use kafka_wire_core::{ApiVersion, KafkaEncode};

use crate::{
    MetadataLimits, RequestError, SaslConfig,
    config::BrokerTemplate,
    reactor::{PollEvent, Poller, broker::BrokerLimits},
    request::erased_request,
};

use super::BrokerSet;

#[test]
fn terminal_authentication_rejection_settles_every_waiting_call_as_not_sent() {
    // Given: two calls wait while the discovered broker authenticates.
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind rejecting broker: {error}"));
    let port = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read rejecting broker address: {error}"))
        .port();
    let directory = directory(port);
    let route = directory
        .route_to(broker_id())
        .unwrap_or_else(|| panic!("known broker route"));
    let mut brokers = broker_set();
    assert!(brokers.install_directory(&directory).is_ok());
    let mut poller = Poller::new(nonzero(4)).unwrap_or_else(|error| panic!("test poller: {error}"));
    let (first_call, first) = request(1);
    let (lane, dns) = brokers
        .submit_route(&poller, route, EffectId::from_raw(1), first, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("first route request: {error}"))
        .unwrap_or_else(|| panic!("first route demand must resolve"));
    let (second_call, second) = request(2);
    assert!(
        brokers
            .submit_route(
                &poller,
                route,
                EffectId::from_raw(2),
                second,
                Moment::ORIGIN,
            )
            .unwrap_or_else(|error| panic!("second route request: {error}"))
            .is_none()
    );
    brokers
        .complete_resolution(
            lane,
            DnsOutcome::new(dns.epoch(), dns.effect_id(), Ok(addresses(port))),
            &poller,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("complete DNS: {error}"));
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept broker connection: {error}"));
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("bound broker read: {error}"));

    // When: negotiation succeeds, then the broker rejects the configured mechanism.
    observe_once(&mut poller, &mut brokers);
    observe_once(&mut poller, &mut brokers);
    read_frame(&mut peer);
    peer.write_all(&negotiation_response())
        .unwrap_or_else(|error| panic!("write negotiation response: {error}"));
    observe_once(&mut poller, &mut brokers);
    observe_once(&mut poller, &mut brokers);
    read_frame(&mut peer);
    peer.write_all(&unsupported_handshake_response())
        .unwrap_or_else(|error| panic!("write rejected handshake: {error}"));
    observe_once(&mut poller, &mut brokers);

    // Then: both unsent calls receive the exact terminal authentication reason.
    let expected = Err(RequestError::Rejected {
        failure: CallFailure::ConnectionClosed {
            reason: CloseReason::AuthenticationFailed(AuthenticationFailure::UnsupportedMechanism),
        },
        delivery: Delivery::NotSent,
    });
    assert_eq!(first_call.wait(), Ok(expected.clone()));
    assert_eq!(second_call.wait(), Ok(expected));
    assert_eq!(brokers.waiting_calls(), 0);
}

fn observe_once(poller: &mut Poller, brokers: &mut BrokerSet) {
    let mut events = Vec::<PollEvent>::with_capacity(4);
    poller
        .poll_into(Some(Duration::from_secs(1)), &mut events)
        .unwrap_or_else(|error| panic!("poll broker readiness: {error}"));
    assert!(
        !events.is_empty(),
        "expected broker readiness before timeout"
    );
    for event in events {
        brokers
            .observe(poller, event, Moment::ORIGIN)
            .unwrap_or_else(|error| panic!("observe broker readiness: {error}"));
    }
}

fn broker_set() -> BrokerSet {
    let sasl = SaslConfig::plain("alice", "s3cret")
        .unwrap_or_else(|error| panic!("valid PLAIN config: {error}"));
    BrokerSet::new(
        BrokerLimits::default(),
        MetadataLimits::new(
            BrokerDirectoryLimits::new(nonzero(1)),
            Duration::from_secs(1),
        )
        .with_waiting_limits(nonzero(2), nonzero(4_096), nonzero(1)),
        Some(BrokerTemplate::plaintext().with_sasl(Some(sasl))),
    )
    .unwrap_or_else(|error| panic!("valid broker set: {error}"))
}

fn request(
    raw_call_id: u64,
) -> (
    crate::Call<Result<kafka_wire::ApiVersionsResponse, RequestError>>,
    Box<dyn crate::request::ErasedRequest>,
) {
    erased_request(
        kafka_driver_core::CallId::from_raw(raw_call_id),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    )
}

fn negotiation_response() -> Vec<u8> {
    let mut response = ApiVersionsResponse::default();
    response.api_keys = vec![
        advertised(&SASL_HANDSHAKE_API_DESCRIPTOR, 1),
        advertised(&API_VERSIONS_API_DESCRIPTOR, 0),
        advertised(&SASL_AUTHENTICATE_API_DESCRIPTOR, 1),
    ];
    encode_response(0, &response, ApiVersion::new(0))
}

fn unsupported_handshake_response() -> Vec<u8> {
    let mut response = SaslHandshakeResponse::default();
    response.error_code = 33;
    encode_response(1, &response, ApiVersion::new(1))
}

fn advertised(descriptor: &kafka_wire::ApiDescriptor, maximum: i16) -> AdvertisedApi {
    let mut api = AdvertisedApi::default();
    api.api_key = descriptor.api_key.value();
    api.min_version = 0;
    api.max_version = maximum;
    api
}

fn encode_response<R: KafkaEncode>(
    correlation_id: i32,
    response: &R,
    body_version: ApiVersion,
) -> Vec<u8> {
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;
    assert!(header.encode_into(&mut body, ApiVersion::new(0)).is_ok());
    assert!(response.encode_into(&mut body, body_version).is_ok());
    let length =
        i32::try_from(body.len()).unwrap_or_else(|error| panic!("response frame length: {error}"));
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

fn read_frame(peer: &mut TcpStream) {
    let mut prefix = [0; size_of::<i32>()];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read frame length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("nonnegative frame length: {error}"));
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read frame body: {error}"));
}

fn directory(port: u16) -> BrokerDirectory {
    let endpoint = BrokerEndpoint::new(
        HostName::new("127.0.0.1").unwrap_or_else(|error| panic!("valid host: {error}")),
        nonzero_port(port),
    );
    BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(1),
        [BrokerDirectoryEntry::new(broker_id(), endpoint)],
        BrokerDirectoryLimits::new(nonzero(1)),
    )
    .unwrap_or_else(|error| panic!("valid directory: {error}"))
}

fn addresses(port: u16) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(
            IpAddress::V4([127, 0, 0, 1]),
            nonzero_port(port),
        )],
        ResolutionLimits::new(nonzero(1)),
    )
    .unwrap_or_else(|error| panic!("valid address set: {error}"))
}

fn broker_id() -> BrokerId {
    BrokerId::new(7).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}

fn nonzero_port(value: u16) -> NonZeroU16 {
    NonZeroU16::new(value).unwrap_or_else(|| panic!("test port must be nonzero"))
}
