//! Exact proof queue capacity and fairness-bounded outcome scenarios.

use std::num::{NonZeroU8, NonZeroUsize};

use kafka_driver_core::{
    AuthenticationRound, ConnectionEpoch, EffectId, ExchangeOutcome, TransportId,
};
use kafka_wire_core::Bytes;

use crate::{
    SaslConfig, ScramProofLimits,
    authentication::AuthenticationSession,
    reactor::resource::{ResourceIdentity, ResourceToken},
};

use super::{ScramProofRequest, ScramProofSubmitError, ScramProofWorker, ScramProofWorkerError};

#[test]
fn request_queue_accepts_exact_capacity_and_returns_one_more_secret_owner() {
    let (worker, requests, _outcomes) = ScramProofWorker::isolated(limits(1, 1, 1));
    let sender = worker.sender();

    assert!(sender.submit(request(1)).is_ok());
    let overflow = sender.submit(request(2));

    assert!(matches!(overflow, Err(ScramProofSubmitError::Full(_))));
    assert!(requests.try_recv().is_ok());
}

#[test]
fn outcome_drain_stops_at_its_turn_budget_and_retains_remaining_work() {
    let (worker, _requests, outcomes) = ScramProofWorker::isolated(limits(1, 3, 2));
    for raw in 1..=3 {
        outcomes
            .send(request(raw).finish())
            .unwrap_or_else(|error| panic!("queue proof outcome: {error}"));
    }
    let mut batch = Vec::new();

    let first = worker
        .drain_into(&mut batch)
        .unwrap_or_else(|error| panic!("drain first proof batch: {error}"));
    assert_eq!(first.outcomes(), 2);
    assert!(first.more_work());
    batch.clear();
    let second = worker
        .drain_into(&mut batch)
        .unwrap_or_else(|error| panic!("drain second proof batch: {error}"));
    assert_eq!(second.outcomes(), 1);
    assert!(!second.more_work());
}

#[test]
fn closed_outcome_channel_reports_lost_worker_instead_of_idle_progress() {
    let (worker, _requests, outcomes) = ScramProofWorker::isolated(limits(1, 1, 1));
    drop(outcomes);

    assert_eq!(
        worker.drain_into(&mut Vec::new()),
        Err(ScramProofWorkerError::Lost)
    );
}

pub(in crate::reactor) fn request(raw: u64) -> ScramProofRequest {
    let config = SaslConfig::scram_sha_256("worker-user", "worker-password")
        .unwrap_or_else(|error| panic!("valid worker config: {error}"));
    let mut session = AuthenticationSession::new(config)
        .unwrap_or_else(|failure| panic!("worker session: {failure:?}"));
    let first = session
        .next_message(1_024)
        .unwrap_or_else(|failure| panic!("worker client first: {failure:?}"));
    let first =
        std::str::from_utf8(&first).unwrap_or_else(|error| panic!("UTF-8 client first: {error}"));
    let nonce = first
        .rsplit_once("r=")
        .map_or_else(|| panic!("client first nonce missing"), |(_, nonce)| nonce);
    let challenge = format!("r={nonce}-server,s=YWJj,i=4096");
    ScramProofRequest::new(
        ResourceToken::new(
            calandria::ResourceOwnerId::new(raw),
            calandria::ResourceSlotId::new(0),
            calandria::ResourceGeneration::INITIAL,
        ),
        ResourceIdentity::new(TransportId::from_raw(raw), ConnectionEpoch::from_raw(raw)),
        EffectId::from_raw(raw),
        AuthenticationRound::new(NonZeroU8::MIN),
        session,
        Bytes::from(challenge),
    )
}

pub(super) fn assert_continues(outcome: super::ScramProofOutcome) {
    let (_, outcome) = outcome.into_parts();
    assert_eq!(outcome, ExchangeOutcome::Continue);
}

fn limits(requests: usize, outcomes: usize, budget: usize) -> ScramProofLimits {
    ScramProofLimits::new(nonzero(requests), nonzero(outcomes), nonzero(budget))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
