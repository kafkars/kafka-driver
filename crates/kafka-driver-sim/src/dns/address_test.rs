//! Address-value scenarios for validated names and socket-free IP vocabulary.

use std::num::NonZeroU16;

use super::{BrokerEndpoint, HostName, HostNameError, IpAddress, ResolvedAddress};

#[test]
fn host_names_reject_empty_and_whitespace_only_values() {
    assert_eq!(HostName::new(""), Err(HostNameError));
    assert_eq!(HostName::new(" \t\n"), Err(HostNameError));

    let Ok(host) = HostName::new("broker.test") else {
        panic!("nonempty test host must be valid");
    };
    assert_eq!(host.as_str(), "broker.test");
    assert_eq!(host.to_string(), "broker.test");
}

#[test]
fn configured_and_resolved_endpoints_retain_explicit_ports() {
    let Ok(host) = HostName::new("broker.test") else {
        panic!("nonempty test host must be valid");
    };
    let endpoint = BrokerEndpoint::new(host, port());
    let address = ResolvedAddress::new(IpAddress::V6([1; 16]), port());

    assert_eq!(endpoint.host().as_str(), "broker.test");
    assert_eq!(endpoint.port(), port());
    assert_eq!(address.ip(), IpAddress::V6([1; 16]));
    assert_eq!(address.port(), port());
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}
