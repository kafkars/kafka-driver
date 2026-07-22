//! Boundary scenarios for shared broker endpoint vocabulary.

use std::num::NonZeroU16;

use crate::{BrokerEndpoint, HostName, HostNameError, IpAddress, ResolvedAddress};

#[test]
fn host_names_reject_empty_and_ambiguous_text() {
    assert_eq!(HostName::new(""), Err(HostNameError::Empty));
    assert_eq!(
        HostName::new("broker name"),
        Err(HostNameError::InvalidCharacter)
    );
    assert_eq!(
        HostName::new("bróker.test"),
        Err(HostNameError::InvalidCharacter)
    );
    assert_eq!(
        HostName::new("broker\0.test"),
        Err(HostNameError::InvalidCharacter)
    );
}

#[test]
fn host_name_capacity_accepts_the_exact_limit_and_rejects_one_more_byte() {
    let exact = "a".repeat(HostName::MAX_BYTES);
    let one_more = "a".repeat(HostName::MAX_BYTES + 1);

    assert_eq!(
        HostName::new(exact.clone()).map(|host| host.as_str().len()),
        Ok(HostName::MAX_BYTES)
    );
    assert_eq!(
        HostName::new(one_more),
        Err(HostNameError::TooLong {
            bytes: HostName::MAX_BYTES + 1,
            limit: HostName::MAX_BYTES,
        })
    );
    assert_eq!(
        HostName::new(exact).map(|host| host.to_string()),
        Ok("a".repeat(HostName::MAX_BYTES))
    );
}

#[test]
fn configured_and_resolved_endpoints_retain_explicit_ports() {
    let host =
        HostName::new("broker.test").unwrap_or_else(|error| panic!("nonempty test host: {error}"));
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
