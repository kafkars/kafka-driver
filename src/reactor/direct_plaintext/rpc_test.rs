//! Loopback proof for the selector-neutral Bornera RPC adapter.

use kafka_wire::{API_VERSIONS_API_DESCRIPTOR, ApiVersionsResponse};
use kafka_wire_core::ApiVersion;

use crate::{
    DriverLimits,
    reactor::{BrokerRpc, causality::CausalSequence},
};

use super::super::shared_set_fixture_test as fixture;
use super::DirectBrokerRpc;

#[test]
fn direct_rpc_reflects_public_admission_and_submits_on_the_owned_lane() {
    let listener = fixture::listener();
    let address = fixture::address(&listener);
    let server = fixture::spawn_lane(listener, None, 0);
    let driver = DriverLimits::default();
    let mut set = fixture::shared_set(&driver);
    let mut lane = fixture::plaintext_lane(&mut set, &driver, address, 1);
    let mut causality = CausalSequence::new();
    {
        let rpc = DirectBrokerRpc::new(set.access(&mut lane), &mut causality);
        assert!(!rpc.is_ready());
    }
    for _ in 0..32 {
        fixture::drive(&mut set, std::slice::from_mut(&mut lane), &mut causality);
        if fixture::ready(&lane) {
            break;
        }
        fixture::wait_if_idle(&mut set, std::slice::from_mut(&mut lane));
    }
    let (call, request) = fixture::request(41);
    {
        let mut rpc = DirectBrokerRpc::new(set.access(&mut lane), &mut causality);
        assert!(rpc.is_ready());
        assert_eq!(
            rpc.negotiated_version(API_VERSIONS_API_DESCRIPTOR.api_key),
            Some(ApiVersion::new(0))
        );
        rpc.submit(request, fixture::NOW)
            .unwrap_or_else(|error| panic!("submit through direct RPC: {error}"));
    }
    let mut result = None;
    for _ in 0..64 {
        fixture::drive(&mut set, std::slice::from_mut(&mut lane), &mut causality);
        result = call.try_result();
        if result.is_some() {
            break;
        }
        fixture::wait_if_idle(&mut set, std::slice::from_mut(&mut lane));
    }

    assert_eq!(result, Some(Ok(Ok(ApiVersionsResponse::default()))));
    server.join().unwrap_or_else(|_| panic!("join RPC broker"));
}
