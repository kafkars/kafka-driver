//! Selector-neutral metadata RPC contract scenarios.

use std::{cell::Cell, io, time::Duration};

use kafka_driver_core::{CallId, EvidenceStamp, Moment, PartitionId, TopicName};
use kafka_wire::{ApiVersionsRequest, METADATA_API_DESCRIPTOR};
use kafka_wire_core::{ApiKey, ApiVersion};

use crate::{
    MetadataLimits,
    api::CallIds,
    request::{ErasedRequest, erased_request},
};

use super::{
    BrokerRpc, BrokerRpcError,
    metadata::{MetadataOwner, MetadataOwnerError, PartitionWait},
};

#[test]
fn metadata_defers_initial_refresh_until_public_admission_is_ready() {
    let mut metadata = MetadataOwner::new(MetadataLimits::default());
    let mut broker = FakeBrokerRpc::not_ready();

    let progress = drive(&mut metadata, &mut broker)
        .unwrap_or_else(|error| panic!("drive unavailable metadata RPC: {error}"));

    assert!(!progress);
    assert_eq!(broker.submissions, 0);
    assert_eq!(broker.version_query.get(), None);
}

#[test]
fn ready_metadata_refresh_uses_the_rpc_negotiated_version_and_submits_once() {
    let mut metadata = MetadataOwner::new(MetadataLimits::default());
    let mut broker = FakeBrokerRpc::ready(ApiVersion::new(4));

    let progress = drive(&mut metadata, &mut broker)
        .unwrap_or_else(|error| panic!("drive ready metadata RPC: {error}"));

    assert!(progress);
    assert_eq!(broker.submissions, 1);
    assert_eq!(
        broker.version_query.get(),
        Some(METADATA_API_DESCRIPTOR.api_key)
    );
}

#[test]
fn queued_fetch_submits_once_after_missing_or_unready_rpc_becomes_ready() {
    for broker_present in [false, true] {
        assert_queued_fetch_reaches_ready_rpc(broker_present);
    }
}

#[test]
fn selector_specific_submission_failure_stays_sanitized_at_metadata_boundary() {
    let mut metadata = MetadataOwner::new(MetadataLimits::default());
    let mut broker = FakeBrokerRpc::ready(ApiVersion::new(4));
    broker.fail_submission = true;

    let Err(error) = drive(&mut metadata, &mut broker) else {
        panic!("failed RPC submission must fail metadata drive");
    };

    assert!(matches!(
        error,
        MetadataOwnerError::Broker(BrokerRpcError::Bornera(_))
    ));
    assert!(!metadata.has_pending_rpc());
}

fn drive(
    metadata: &mut MetadataOwner,
    broker: &mut dyn BrokerRpc,
) -> Result<bool, MetadataOwnerError> {
    metadata.drive(
        broker,
        Moment::ORIGIN,
        &CallIds::new(),
        EvidenceStamp::ORIGIN,
    )
}

fn assert_queued_fetch_reaches_ready_rpc(broker_present: bool) {
    let mut metadata = MetadataOwner::new(MetadataLimits::default());
    let mut broker = FakeBrokerRpc::not_ready();
    let call_ids = CallIds::new();
    let (_call, request) = erased_request(
        CallId::from_raw(91),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );
    let topic = TopicName::new("orders")
        .unwrap_or_else(|error| panic!("construct queued metadata topic: {error}"));
    let partition = PartitionId::new(0)
        .unwrap_or_else(|error| panic!("construct queued metadata partition: {error}"));
    let rpc = broker_present.then_some(&mut broker as &mut dyn BrokerRpc);
    metadata
        .wait_for_partition(
            PartitionWait::new(topic, partition, request),
            rpc,
            Moment::ORIGIN,
            &call_ids,
            EvidenceStamp::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("queue metadata fetch without ready RPC: {error}"));
    assert_eq!(broker.submissions, 0);
    broker.ready = true;
    broker.version = ApiVersion::new(4);

    assert!(
        drive(&mut metadata, &mut broker)
            .unwrap_or_else(|error| panic!("submit queued metadata fetch: {error}"))
    );
    assert_eq!(broker.submissions, 1);
    assert_eq!(
        broker.version_query.get(),
        Some(METADATA_API_DESCRIPTOR.api_key)
    );
}

struct FakeBrokerRpc {
    ready: bool,
    version: ApiVersion,
    version_query: Cell<Option<ApiKey>>,
    submissions: usize,
    fail_submission: bool,
}

impl FakeBrokerRpc {
    const fn not_ready() -> Self {
        Self {
            ready: false,
            version: ApiVersion::new(0),
            version_query: Cell::new(None),
            submissions: 0,
            fail_submission: false,
        }
    }

    const fn ready(version: ApiVersion) -> Self {
        Self {
            ready: true,
            version,
            version_query: Cell::new(None),
            submissions: 0,
            fail_submission: false,
        }
    }
}

impl BrokerRpc for FakeBrokerRpc {
    fn is_ready(&self) -> bool {
        self.ready
    }

    fn negotiated_version(&self, api_key: ApiKey) -> Option<ApiVersion> {
        self.version_query.set(Some(api_key));
        Some(self.version)
    }

    fn submit(
        &mut self,
        _request: Box<dyn ErasedRequest>,
        _now: Moment,
    ) -> Result<(), BrokerRpcError> {
        self.submissions += 1;
        if self.fail_submission {
            return Err(BrokerRpcError::Bornera(io::Error::other(
                "injected metadata submission failure",
            )));
        }
        Ok(())
    }
}
