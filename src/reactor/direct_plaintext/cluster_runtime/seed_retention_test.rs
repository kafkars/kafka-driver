//! Runtime-owned bootstrap evidence supersession, retry, and terminal cleanup.

use std::{cell::Cell, time::Duration};

use kafka_driver_core::{CallFailure, CallId, ConnectionEpoch, Delivery};
use kafka_wire::ApiVersionsRequest;

use crate::{RequestError, reactor::causality::CausalSequence, request::erased_request};

use super::{
    ResolvedSeedReplacement,
    adapter_test::{CountingFactory, FailingFactory, failed_plan, runtime, seed},
};
use crate::reactor::direct_plaintext::cluster_runtime::seed_rotation::test::failed_plan as rotating_plan;

const NOW: kafka_driver_core::Moment = kafka_driver_core::Moment::from_nanos(1);

#[test]
fn newer_pending_generation_supersedes_without_factory_or_identity_use() {
    let mut runtime = runtime();
    let owner = runtime
        .install_seed(ConnectionEpoch::from_raw(1), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("install retained seed: {error}"));
    let factory = CountingFactory {
        attempts: Cell::new(0),
    };

    assert_retained(runtime.replace_resolved_seed(&factory, seed(2, "two.test", 9002), NOW));
    assert_retained(runtime.replace_resolved_seed(&factory, seed(4, "four.test", 9004), NOW));
    assert!(matches!(
        runtime
            .replace_resolved_seed(&factory, seed(3, "three.test", 9003), NOW)
            .unwrap_or_else(|error| panic!("drop stale pending seed: {error}")),
        ResolvedSeedReplacement::Stale
    ));
    assert_eq!(factory.attempts.get(), 0);
    assert_eq!(
        pending_generation(&runtime),
        Some(ConnectionEpoch::from_raw(4))
    );
    let (_, [next]) = runtime
        .reserve_endpoint_lanes::<1>()
        .unwrap_or_else(|error| panic!("reserve after retained seeds: {error}"));
    assert_eq!(next.lane().get(), owner.lane().get() + 1);
}

#[test]
fn restart_pending_is_idle_and_resolution_owned_retries_once_on_drive() {
    let mut runtime = runtime();
    runtime
        .install_seed(ConnectionEpoch::from_raw(1), rotating_plan(), NOW)
        .unwrap_or_else(|error| panic!("install rotating seed: {error}"));
    let mut causality = CausalSequence::new();
    assert!(
        runtime
            .prepare_seed_bootstrap_restart(NOW, &mut causality)
            .unwrap_or_else(|error| panic!("prepare seed rotation: {error}"))
    );
    let factory = CountingFactory {
        attempts: Cell::new(0),
    };
    assert_retained(runtime.replace_resolved_seed(&factory, seed(2, "next.test", 9012), NOW));
    assert!(!runtime.has_local_work());
    assert!(
        !runtime
            .drive_with_factory(&factory, NOW, &mut causality)
            .unwrap_or_else(|error| panic!("drive blocked seed: {error}"))
    );
    assert_eq!(factory.attempts.get(), 0);

    runtime
        .mark_seed_bootstrap_resolution_owned()
        .unwrap_or_else(|error| panic!("own seed resolution: {error}"));
    assert!(
        runtime
            .drive_with_factory(&factory, NOW, &mut causality)
            .unwrap_or_else(|error| panic!("retry retained seed: {error}"))
    );
    assert_eq!(factory.attempts.get(), 1);
    assert_eq!(pending_generation(&runtime), None);
    assert_eq!(
        runtime.seed.map(|installed| installed.generation),
        Some(ConnectionEpoch::from_raw(2))
    );
    runtime
        .drive_with_factory(&factory, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("drive after seed replacement: {error}"));
    assert_eq!(factory.attempts.get(), 1);
}

#[test]
fn fatal_retry_clears_evidence_and_totalizes_external_waiters() {
    let mut runtime = runtime();
    runtime
        .install_seed(ConnectionEpoch::from_raw(1), rotating_plan(), NOW)
        .unwrap_or_else(|error| panic!("install fatal-retry seed: {error}"));
    let mut causality = CausalSequence::new();
    let (call, request) = request(7);
    runtime
        .submit_seed(request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("retain seed waiter: {error}"));
    runtime
        .prepare_seed_bootstrap_restart(NOW, &mut causality)
        .unwrap_or_else(|error| panic!("prepare fatal retry: {error}"));
    let factory = FailingFactory {
        attempts: Cell::new(0),
    };
    assert_retained(runtime.replace_resolved_seed(&factory, seed(2, "fatal.test", 9022), NOW));
    runtime
        .mark_seed_bootstrap_resolution_owned()
        .unwrap_or_else(|error| panic!("own fatal retry: {error}"));

    let error = runtime
        .drive_with_factory(&factory, NOW, &mut causality)
        .err()
        .unwrap_or_else(|| panic!("fatal seed factory must fail the host"));
    assert_eq!(error.to_string(), "synthetic seed factory failure");
    assert_eq!(factory.attempts.get(), 1);
    assert_eq!(pending_generation(&runtime), None);
    assert_eq!(call.try_result(), Some(Ok(Err(closed()))));
}

#[test]
fn aggregate_drain_discards_pending_evidence_and_cannot_reopen_it() {
    let mut runtime = runtime();
    runtime
        .install_seed(ConnectionEpoch::from_raw(1), rotating_plan(), NOW)
        .unwrap_or_else(|error| panic!("install draining seed: {error}"));
    runtime
        .prepare_seed_bootstrap_restart(NOW, &mut CausalSequence::new())
        .unwrap_or_else(|error| panic!("prepare draining seed: {error}"));
    let factory = CountingFactory {
        attempts: Cell::new(0),
    };
    assert_retained(runtime.replace_resolved_seed(&factory, seed(2, "drain.test", 9032), NOW));
    runtime.begin_seed_waiting_drain();
    assert_eq!(pending_generation(&runtime), None);
    assert!(matches!(
        runtime
            .replace_resolved_seed(&factory, seed(3, "late.test", 9033), NOW)
            .unwrap_or_else(|error| panic!("discard post-drain seed: {error}")),
        ResolvedSeedReplacement::Stale
    ));
    assert!(
        !runtime
            .retry_pending_resolved_seed(&factory, NOW)
            .unwrap_or_else(|error| panic!("observe empty seed retry: {error}"))
    );
    assert_eq!(factory.attempts.get(), 0);
    assert!(!runtime.has_local_work());
}

fn assert_retained(result: std::io::Result<ResolvedSeedReplacement>) {
    assert!(matches!(
        result.unwrap_or_else(|error| panic!("retain pending seed: {error}")),
        ResolvedSeedReplacement::Retained
    ));
}

fn pending_generation<T: bornera::RegisteredTransport>(
    runtime: &super::super::ClusterRuntime<T>,
) -> Option<ConnectionEpoch> {
    runtime
        .pending_resolved_seed
        .as_ref()
        .map(crate::reactor::bootstrap::ResolvedSeed::generation)
}

fn request(
    raw: u64,
) -> (
    crate::Call<Result<kafka_wire::ApiVersionsResponse, RequestError>>,
    Box<dyn crate::request::ErasedRequest>,
) {
    erased_request(
        CallId::from_raw(raw),
        ApiVersionsRequest::default(),
        Duration::from_secs(5),
    )
}

fn closed() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Closed,
        delivery: Delivery::NotSent,
    }
}
