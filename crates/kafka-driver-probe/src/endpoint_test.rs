//! Scenarios for operator endpoint validation before real I/O begins.

use super::endpoint::{EndpointError, bootstrap};

#[test]
fn dns_ipv4_and_bracketed_ipv6_hosts_enter_one_bounded_bootstrap_set() {
    for value in ["broker.test:9092", "127.0.0.1:9092", "[::1]:9092"] {
        let endpoints = bootstrap(value)
            .unwrap_or_else(|error| panic!("valid endpoint {value} rejected: {error}"));

        assert_eq!(endpoints.len(), 1);
    }
}

#[test]
fn missing_hosts_ambiguous_ipv6_and_zero_ports_are_rejected() {
    assert_eq!(bootstrap(":9092"), Err(EndpointError::Shape));
    assert_eq!(bootstrap("::1:9092"), Err(EndpointError::Shape));
    assert_eq!(bootstrap("broker.test:0"), Err(EndpointError::Port));
    assert_eq!(bootstrap("broker.test:many"), Err(EndpointError::Port));
}
