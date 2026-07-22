//! Real-loop proof that dedicated hosting preserves graceful FIFO drain.

use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver::{ApiVersion, Call, Delivery, Driver, RequestError};
use kafka_driver_core::CallFailure;
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse,
    FIND_COORDINATOR_API_DESCRIPTOR, METADATA_API_DESCRIPTOR, ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi, response_header_version_for,
};
use kafka_wire_core::KafkaEncode;

#[test]
fn dedicated_shutdown_waits_for_the_in_flight_fifo_response_before_join() {
    // Given
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind dedicated loopback broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read dedicated broker address: {error}"));
    let (driver, host) = Driver::builder()
        .broker(address)
        .spawn()
        .unwrap_or_else(|error| panic!("spawn configured dedicated host: {error}"));
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept dedicated driver connection: {error}"));
    complete_dedicated_negotiation(&mut peer);
    let response = ApiVersionsResponse::default();
    let call = admit_ready_call(&driver, &mut peer);
    let shutdown = driver
        .shutdown()
        .unwrap_or_else(|error| panic!("admit dedicated graceful shutdown: {error}"));
    let probe = driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("admit post-shutdown probe: {error}"));
    assert_eq!(
        probe.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::Draining,
            delivery: Delivery::NotSent,
        }))
    );
    assert!(!host.is_finished());

    // When
    peer.write_all(&encoded_response(&response))
        .unwrap_or_else(|error| panic!("write dedicated generated response: {error}"));

    // Then
    assert_eq!(call.wait(), Ok(Ok(response)));
    assert_eq!(shutdown.wait(), Ok(()));
    assert!(host.join().is_ok());
}

fn admit_ready_call(
    driver: &Driver,
    peer: &mut TcpStream,
) -> Call<Result<ApiVersionsResponse, RequestError>> {
    for _ in 0..3 {
        let call = driver
            .call(ApiVersionsRequest::default(), Duration::from_secs(5))
            .unwrap_or_else(|error| panic!("admit dedicated generated call: {error}"));
        match read_frame(peer) {
            Ok(()) => return call,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                assert_eq!(
                    call.wait(),
                    Ok(Err(RequestError::Rejected {
                        failure: CallFailure::NotReady,
                        delivery: Delivery::NotSent,
                    }))
                );
            }
            Err(error) => panic!("read dedicated generated request: {error}"),
        }
    }
    panic!("dedicated connection did not become ready within bounded probes")
}

fn read_frame(peer: &mut TcpStream) -> io::Result<()> {
    let mut prefix = [0; size_of::<i32>()];
    peer.read_exact(&mut prefix)?;
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("nonnegative dedicated request length: {error}"));
    let mut body = vec![0; length];
    peer.read_exact(&mut body)?;
    assert!(!body.is_empty());
    Ok(())
}

fn complete_dedicated_negotiation(peer: &mut TcpStream) {
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("bound dedicated broker read: {error}"));
    read_frame(peer).unwrap_or_else(|error| panic!("read dedicated negotiation: {error}"));
    peer.write_all(&negotiation_response())
        .unwrap_or_else(|error| panic!("write dedicated negotiation response: {error}"));
}

fn negotiation_response() -> Vec<u8> {
    let mut response = ApiVersionsResponse::default();
    response
        .api_keys
        .push(advertisement(METADATA_API_DESCRIPTOR.api_key.value(), 1));
    response.api_keys.push(advertisement(
        API_VERSIONS_API_DESCRIPTOR.api_key.value(),
        0,
    ));
    response.api_keys.push(advertisement(
        FIND_COORDINATOR_API_DESCRIPTOR.api_key.value(),
        3,
    ));

    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = 0;
    header
        .encode_into(&mut body, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("encode dedicated negotiation header: {error}"));
    response
        .encode_into(&mut body, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("encode dedicated negotiation body: {error}"));
    frame(&body, "dedicated negotiation")
}

fn advertisement(api_key: i16, max_version: i16) -> AdvertisedApi {
    let mut api = AdvertisedApi::default();
    api.api_key = api_key;
    api.min_version = 0;
    api.max_version = max_version;
    api
}

fn encoded_response(response: &ApiVersionsResponse) -> Vec<u8> {
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = 0;
    let header_version = response_header_version_for::<ApiVersionsRequest>(version()).map_or_else(
        |error| panic!("dedicated response header policy: {error}"),
        ApiVersion::new,
    );
    header
        .encode_into(&mut body, header_version)
        .unwrap_or_else(|error| panic!("encode dedicated response header: {error}"));
    response
        .encode_into(&mut body, version())
        .unwrap_or_else(|error| panic!("encode dedicated response body: {error}"));
    frame(&body, "dedicated generated response")
}

fn frame(body: &BytesMut, label: &str) -> Vec<u8> {
    let length =
        i32::try_from(body.len()).unwrap_or_else(|error| panic!("{label} frame length: {error}"));
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(body);
    frame
}

const fn version() -> ApiVersion {
    ApiVersion::new(0)
}
