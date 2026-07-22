//! Focused real-worker scenario for identity, wake, and numeric address fidelity.

use std::{num::NonZeroU16, time::Duration};

use kafka_driver_core::{
    BrokerEndpoint, ConnectionEpoch, DnsRequest, EffectId, HostName, IpAddress,
};

use crate::{ResolverLimits, reactor::Poller};

use super::Resolver;

#[test]
fn numeric_resolution_wakes_the_reactor_with_exact_request_identity() {
    let mut poller = Poller::new(std::num::NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("create test poller: {error}"));
    let wake = crate::reactor::WakeHandle::new(poller.wake_handle());
    let resolver = Resolver::spawn(ResolverLimits::default(), wake)
        .unwrap_or_else(|error| panic!("spawn DNS worker: {error}"));
    let request = DnsRequest::new(
        ConnectionEpoch::from_raw(7),
        EffectId::from_raw(11),
        endpoint(),
    );

    resolver
        .submit(request.clone())
        .unwrap_or_else(|error| panic!("admit DNS request: {error}"));
    let mut events = Vec::new();
    poller
        .poll_into(Some(Duration::from_secs(1)), &mut events)
        .unwrap_or_else(|error| panic!("wait for DNS worker wake: {error}"));
    let mut outcomes = Vec::new();
    let progress = resolver.drain_into(&mut outcomes);

    assert_eq!(progress.outcomes(), 1);
    assert!(!progress.more_work());
    assert_eq!(outcomes[0].epoch(), request.epoch());
    assert_eq!(outcomes[0].effect_id(), request.effect_id());
    let addresses = outcomes[0]
        .result()
        .as_ref()
        .unwrap_or_else(|failure| panic!("numeric resolution must succeed: {failure:?}"));
    assert_eq!(addresses.len(), 1);
    assert_eq!(
        addresses.iter().next().map(|address| address.ip()),
        Some(IpAddress::V4([127, 0, 0, 1]))
    );
}

fn endpoint() -> BrokerEndpoint {
    let host = HostName::new("127.0.0.1")
        .unwrap_or_else(|error| panic!("numeric host must be valid: {error}"));
    BrokerEndpoint::new(host, port())
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}
