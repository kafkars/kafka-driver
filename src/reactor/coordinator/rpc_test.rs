//! Selector-neutral coordinator discovery RPC contract scenarios.

use std::{cell::Cell, error::Error as _, io, time::Duration};

use kafka_driver_core::{
    CallId, CoordinatorKey, CoordinatorKind, CoordinatorState, EvidenceStamp, Moment,
};
use kafka_wire::{ApiVersionsRequest, FIND_COORDINATOR_API_DESCRIPTOR};
use kafka_wire_core::{ApiKey, ApiVersion};

use crate::{
    CoordinatorLimits,
    api::CallIds,
    reactor::{BrokerRpc, BrokerRpcError},
    request::{ErasedRequest, erased_request},
};

use super::{CoordinatorOwner, CoordinatorOwnerError, CoordinatorWait};

#[test]
fn missing_or_unsupported_negotiated_version_rejects_without_submission() {
    for (kind, version) in [
        (CoordinatorKind::Group, None),
        (CoordinatorKind::Share, Some(ApiVersion::new(5))),
    ] {
        let key = key(kind);
        let mut owner = CoordinatorOwner::new(CoordinatorLimits::default());
        let mut broker = FakeBrokerRpc::ready(version);
        let (_call, waiting) = waiting(key.clone(), 91);

        owner
            .wait_for(
                waiting,
                Some(&mut broker),
                Moment::ORIGIN,
                &CallIds::new(),
                EvidenceStamp::ORIGIN,
            )
            .unwrap_or_else(|error| panic!("reject unsupported coordinator RPC: {error}"));

        assert_eq!(broker.submit_attempts, 0);
        assert_eq!(
            broker.version_query.get(),
            Some(FIND_COORDINATOR_API_DESCRIPTOR.api_key)
        );
        assert!(!owner.discovery_pending(&key));
        assert!(owner.entries[0].pending.is_none());
        assert!(matches!(
            owner.entries[0].machine.state(),
            CoordinatorState::Unknown { .. }
        ));
    }
}

#[test]
fn supported_negotiated_version_submits_exactly_once() {
    let mut owner = CoordinatorOwner::new(CoordinatorLimits::default());
    let mut broker = FakeBrokerRpc::ready(Some(ApiVersion::new(6)));
    let (_call, waiting) = waiting(key(CoordinatorKind::Share), 92);
    let call_ids = CallIds::new();

    owner
        .wait_for(
            waiting,
            Some(&mut broker),
            Moment::ORIGIN,
            &call_ids,
            EvidenceStamp::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("submit supported coordinator RPC: {error}"));
    let progress = owner
        .drive(
            &mut broker,
            Moment::ORIGIN,
            &call_ids,
            EvidenceStamp::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("drive pending coordinator RPC: {error}"));

    assert!(!progress);
    assert_eq!(broker.submit_attempts, 1);
    assert_eq!(broker.requests.len(), 1);
    assert_eq!(
        broker.version_query.get(),
        Some(FIND_COORDINATOR_API_DESCRIPTOR.api_key)
    );
    assert_eq!(
        owner.entries[0]
            .pending
            .as_ref()
            .map(|pending| pending.version),
        Some(ApiVersion::new(6))
    );
}

#[test]
fn selector_submission_failure_has_no_false_pending_owner() {
    let mut owner = CoordinatorOwner::new(CoordinatorLimits::default());
    let mut broker = FakeBrokerRpc::ready(Some(ApiVersion::new(3)));
    broker.fail_submission = true;
    let (_call, waiting) = waiting(key(CoordinatorKind::Group), 93);

    let Err(error) = owner.wait_for(
        waiting,
        Some(&mut broker),
        Moment::ORIGIN,
        &CallIds::new(),
        EvidenceStamp::ORIGIN,
    ) else {
        panic!("failed coordinator RPC submission must fail admission");
    };

    assert_eq!(error.to_string(), "coordinator broker submission failed");
    let rpc_source = error
        .source()
        .unwrap_or_else(|| panic!("coordinator error must retain RPC source"));
    assert_eq!(rpc_source.to_string(), "Bornera broker RPC failed");
    assert!(rpc_source.source().is_some());
    assert!(matches!(
        &error,
        CoordinatorOwnerError::Broker(BrokerRpcError::Bornera(_))
    ));
    assert_eq!(broker.submit_attempts, 1);
    assert!(broker.requests.is_empty());
    assert!(owner.entries[0].pending.is_none());
}

#[test]
fn absent_or_unready_rpc_defers_then_submits_once_when_ready() {
    for broker_present in [false, true] {
        assert_deferred_discovery_reaches_ready_rpc(broker_present);
    }
}

fn assert_deferred_discovery_reaches_ready_rpc(broker_present: bool) {
    let key = key(CoordinatorKind::Group);
    let mut owner = CoordinatorOwner::new(CoordinatorLimits::default());
    let mut broker = FakeBrokerRpc::not_ready();
    let (_call, waiting) = waiting(key.clone(), 94);
    let call_ids = CallIds::new();
    let rpc = broker_present.then_some(&mut broker as &mut dyn BrokerRpc);

    owner
        .wait_for(
            waiting,
            rpc,
            Moment::ORIGIN,
            &call_ids,
            EvidenceStamp::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("defer coordinator discovery: {error}"));
    assert_eq!(broker.submit_attempts, 0);
    assert_eq!(broker.version_query.get(), None);
    assert!(owner.discovery_pending(&key));

    broker.ready = true;
    broker.version = Some(ApiVersion::new(3));
    assert!(
        owner
            .drive(
                &mut broker,
                Moment::ORIGIN,
                &call_ids,
                EvidenceStamp::ORIGIN,
            )
            .unwrap_or_else(|error| panic!("start deferred coordinator RPC: {error}"))
    );
    assert_eq!(broker.submit_attempts, 1);
    assert_eq!(broker.requests.len(), 1);
}

fn waiting(
    key: CoordinatorKey,
    raw_call_id: u64,
) -> (
    crate::Call<Result<kafka_wire::ApiVersionsResponse, crate::RequestError>>,
    CoordinatorWait,
) {
    let (call, request) = erased_request(
        CallId::from_raw(raw_call_id),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );
    (call, CoordinatorWait::new(key, request))
}

fn key(kind: CoordinatorKind) -> CoordinatorKey {
    CoordinatorKey::new(kind, "orders-readers")
        .unwrap_or_else(|error| panic!("valid coordinator key rejected: {error}"))
}

struct FakeBrokerRpc {
    ready: bool,
    version: Option<ApiVersion>,
    version_query: Cell<Option<ApiKey>>,
    submit_attempts: usize,
    fail_submission: bool,
    requests: Vec<Box<dyn ErasedRequest>>,
}

impl FakeBrokerRpc {
    const fn not_ready() -> Self {
        Self {
            ready: false,
            version: None,
            version_query: Cell::new(None),
            submit_attempts: 0,
            fail_submission: false,
            requests: Vec::new(),
        }
    }

    const fn ready(version: Option<ApiVersion>) -> Self {
        Self {
            ready: true,
            version,
            version_query: Cell::new(None),
            submit_attempts: 0,
            fail_submission: false,
            requests: Vec::new(),
        }
    }
}

impl BrokerRpc for FakeBrokerRpc {
    fn is_ready(&self) -> bool {
        self.ready
    }

    fn negotiated_version(&self, api_key: ApiKey) -> Option<ApiVersion> {
        self.version_query.set(Some(api_key));
        self.version
    }

    fn submit(
        &mut self,
        request: Box<dyn ErasedRequest>,
        _now: Moment,
    ) -> Result<(), BrokerRpcError> {
        self.submit_attempts += 1;
        if self.fail_submission {
            return Err(BrokerRpcError::Bornera(io::Error::other(
                "injected coordinator submission failure",
            )));
        }
        self.requests.push(request);
        Ok(())
    }
}
