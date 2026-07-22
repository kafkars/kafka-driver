//! Scenarios for bounded stable bootstrap membership and selection.

use std::num::{NonZeroU16, NonZeroUsize};

use crate::{BrokerEndpoint, HostName};

use super::{BootstrapCursor, BootstrapError, BootstrapLimits, BootstrapSet};

#[test]
fn configured_order_is_stable_and_duplicate_endpoints_are_coalesced() {
    let endpoints = BootstrapSet::try_from_iter(
        [
            endpoint("two.test"),
            endpoint("one.test"),
            endpoint("two.test"),
        ],
        BootstrapLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid bootstrap set: {error}"));

    let hosts = endpoints
        .iter()
        .map(|endpoint| endpoint.host().as_str())
        .collect::<Vec<_>>();

    assert_eq!(hosts, ["two.test", "one.test"]);
    assert_eq!(endpoints.len(), 2);
    assert!(!endpoints.is_empty());
}

#[test]
fn bootstrap_admission_accepts_exact_input_capacity_and_rejects_one_more() {
    let limits = BootstrapLimits::new(nonzero_size(2));
    let exact = BootstrapSet::try_from_iter([endpoint("one.test"), endpoint("two.test")], limits);
    let overflow = BootstrapSet::try_from_iter(
        [
            endpoint("one.test"),
            endpoint("two.test"),
            endpoint("three.test"),
        ],
        limits,
    );

    assert_eq!(exact.map(|endpoints| endpoints.len()), Ok(2));
    assert_eq!(overflow, Err(BootstrapError::Capacity { limit: 2 }));
}

#[test]
fn empty_bootstrap_configuration_is_rejected() {
    assert_eq!(
        BootstrapSet::try_from_iter([], BootstrapLimits::default()),
        Err(BootstrapError::Empty)
    );
}

#[test]
fn selection_rotates_through_distinct_endpoints() {
    let endpoints = BootstrapSet::try_from_iter(
        [endpoint("one.test"), endpoint("two.test")],
        BootstrapLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid bootstrap set: {error}"));
    let mut cursor = BootstrapCursor::default();

    assert_eq!(cursor.select_next(&endpoints).host().as_str(), "one.test");
    assert_eq!(cursor.select_next(&endpoints).host().as_str(), "two.test");
    assert_eq!(cursor.select_next(&endpoints).host().as_str(), "one.test");
}

fn endpoint(host: &str) -> BrokerEndpoint {
    let host = HostName::new(host).unwrap_or_else(|error| panic!("valid host: {error}"));
    BrokerEndpoint::new(host, port())
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}

fn nonzero_size(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
