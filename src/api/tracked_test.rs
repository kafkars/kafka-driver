//! Scenarios for exact route publication and routed result observation.

use std::num::{NonZeroU16, NonZeroUsize};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, MetadataGeneration, OutcomeStamp,
};

use crate::{Call, completion::completion_pair};

use super::{RouteReceipt, RoutedCall, RoutedOutcome};

#[test]
fn completed_request_returns_the_exact_route_published_before_it() {
    let receipt = controller_receipt(7);
    let (receiver, completion) = completion_pair();
    let call = RoutedCall::new(Call::new(receiver));
    assert!(
        completion
            .complete(RoutedOutcome::new(Ok("response"), Some(receipt.clone())))
            .is_ok()
    );

    let outcome = call
        .wait()
        .unwrap_or_else(|error| panic!("routed result must complete: {error}"));

    assert_eq!(outcome.result(), &Ok("response"));
    assert_eq!(outcome.receipt(), Some(&receipt));
}

fn controller_receipt(raw_generation: u64) -> RouteReceipt {
    let broker_id = BrokerId::new(1).unwrap_or_else(|error| panic!("valid broker ID: {error}"));
    let host =
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid broker host: {error}"));
    let port = NonZeroU16::new(9092).unwrap_or_else(|| panic!("test port must be nonzero"));
    let directory = BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(raw_generation),
        [BrokerDirectoryEntry::new(
            broker_id,
            BrokerEndpoint::new(host, port),
        )],
        BrokerDirectoryLimits::new(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("valid broker directory: {error}"));
    RouteReceipt::Controller {
        route: directory
            .route_to(broker_id)
            .unwrap_or_else(|| panic!("directory must issue broker route")),
        observed_at: OutcomeStamp::from_raw(9),
    }
}
