//! Generated framing scenarios for the bounded bootstrap exchange.

use bytes::BytesMut;
use kafka_driver_core::{CorrelationId, EffectId};
use kafka_driver_transport::{FrameBody, FrameDecoder, FrameLimits};
use kafka_wire::{ApiVersionsResponse, OutboundFrameLimits, ResponseHeader};
use kafka_wire_core::{ApiVersion, Bytes, DecodeLimits, KafkaEncode};

use super::{NegotiationExchange, NegotiationExchangeError};

#[test]
fn given_matching_generated_response_when_finished_then_typed_body_is_returned() {
    // Given
    let response = ApiVersionsResponse::default();
    let (exchange, request) = start();
    assert!(request.len() > size_of::<i32>());

    // When
    let result = exchange.finish(response_frame(7, &response));

    // Then
    assert_eq!(result, Ok(response));
}

#[test]
fn given_wrong_correlation_when_finished_then_the_frame_is_rejected() {
    // Given
    let response = ApiVersionsResponse::default();
    let (exchange, _) = start();

    // When
    let result = exchange.finish(response_frame(8, &response));

    // Then
    assert_eq!(
        result,
        Err(NegotiationExchangeError::Correlation {
            expected: CorrelationId::from_raw(7),
            observed: CorrelationId::from_raw(8),
        })
    );
}

fn start() -> (NegotiationExchange, Bytes) {
    NegotiationExchange::start(
        EffectId::from_raw(3),
        CorrelationId::from_raw(7),
        OutboundFrameLimits::new(1_024),
        DecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("bootstrap request must encode: {error}"))
}

fn response_frame(correlation: i32, response: &ApiVersionsResponse) -> FrameBody {
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation;
    assert!(
        header.encode_into(&mut body, ApiVersion::new(0)).is_ok(),
        "bootstrap response header must encode"
    );
    assert!(
        response.encode_into(&mut body, ApiVersion::new(0)).is_ok(),
        "bootstrap response body must encode"
    );
    let Ok(length) = i32::try_from(body.len()) else {
        panic!("bootstrap response must fit a Kafka frame");
    };
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    let mut decoder = FrameDecoder::new(FrameLimits::default());
    assert!(decoder.feed(&frame).is_ok());
    let Ok(Some(frame)) = decoder.next_frame() else {
        panic!("complete bootstrap frame must decode");
    };
    frame
}
