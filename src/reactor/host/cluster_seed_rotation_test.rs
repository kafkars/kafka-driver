//! Test-only composition of bootstrap resolver failure and cluster waiter totality.

use kafka_driver_core::{CallFailure, ConnectionEpoch, Delivery, DnsOutcome, DnsRequest, Moment};

use crate::{RequestError, reactor::direct_plaintext::ClusterSeedFatalFixture};

use super::NameResolution;
use super::resolution_test::{addresses, bootstrap_membership, endpoint, lane, resolver_limits};

#[test]
fn resolver_restart_failure_totalizes_cluster_seed_waiters() {
    let (mut resolution, requests, outcomes) =
        NameResolution::isolated(bootstrap_membership(), resolver_limits());
    let first = requests
        .try_recv()
        .unwrap_or_else(|error| panic!("initial bootstrap request: {error}"));
    outcomes
        .send(DnsOutcome::new(
            first.epoch(),
            first.effect_id(),
            Ok(addresses()),
        ))
        .unwrap_or_else(|error| panic!("complete initial bootstrap: {error}"));
    resolution
        .drive_for_test(&mut Vec::new(), &mut Vec::new(), Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("install initial bootstrap seed: {error}"));
    drop(requests);
    let resolution_error = resolution
        .restart_bootstrap()
        .err()
        .unwrap_or_else(|| panic!("closed resolver must fail bootstrap restart"));
    let (mut cluster, call) = ClusterSeedFatalFixture::waiting();

    let error = cluster.totalize_host_error(std::io::Error::other(resolution_error));

    assert_eq!(error.to_string(), "resolver worker is closed");
    assert_eq!(
        call.try_result(),
        Some(Ok(Err(RequestError::Rejected {
            failure: CallFailure::Closed,
            delivery: Delivery::NotSent,
        })))
    );
}

#[test]
fn full_worker_queue_keeps_rotated_bootstrap_owned_until_dispatch() {
    let (mut resolution, requests, outcomes) =
        NameResolution::isolated(bootstrap_membership(), resolver_limits());
    let first = requests
        .try_recv()
        .unwrap_or_else(|error| panic!("initial bootstrap request: {error}"));
    outcomes
        .send(DnsOutcome::new(
            first.epoch(),
            first.effect_id(),
            Ok(addresses()),
        ))
        .unwrap_or_else(|error| panic!("complete initial bootstrap: {error}"));
    resolution
        .drive_for_test(&mut Vec::new(), &mut Vec::new(), Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("install initial bootstrap seed: {error}"));

    let permit = resolution
        .try_reserve_broker(lane())
        .unwrap_or_else(|error| panic!("reserve competing DNS owner: {error}"))
        .unwrap_or_else(|| panic!("competing DNS owner must fit"));
    let competing = DnsRequest::new(ConnectionEpoch::from_raw(9), permit.effect_id(), endpoint());
    resolution
        .submit(permit, competing.clone())
        .unwrap_or_else(|error| panic!("occupy resolver request queue: {error}"));
    let (mut cluster, call) = ClusterSeedFatalFixture::waiting();

    assert!(
        resolution
            .restart_bootstrap()
            .unwrap_or_else(|error| panic!("retain rotated bootstrap request: {error}"))
    );
    cluster.mark_resolution_owned();
    assert!(call.try_result().is_none());
    assert_eq!(requests.try_recv(), Ok(competing));
    let progress = resolution
        .drive_for_test(&mut Vec::new(), &mut Vec::new(), Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("retry retained bootstrap request: {error}"));
    assert_eq!(progress.submissions, 1);
    let rotated = requests
        .try_recv()
        .unwrap_or_else(|error| panic!("rotated bootstrap request: {error}"));
    assert_eq!(rotated.endpoint().host().as_str(), "127.0.0.2");
    assert_eq!(rotated.epoch(), ConnectionEpoch::from_raw(2));
    assert!(call.try_result().is_none());

    let _ = cluster.totalize_host_error(std::io::Error::other("test cleanup"));
}
