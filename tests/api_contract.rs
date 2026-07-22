//! Public scenario proving kafka-wire supplies typed response pairing.

use kafka_driver::RequestResponsePair;
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};

fn assert_response_pair<Request, Response>()
where
    Request: RequestResponsePair<Response = Response>,
{
}

#[test]
fn api_versions_request_names_its_generated_response() {
    assert_response_pair::<ApiVersionsRequest, ApiVersionsResponse>();
}
