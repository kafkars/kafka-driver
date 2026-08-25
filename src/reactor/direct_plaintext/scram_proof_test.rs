//! Direct proof identity, pressure, deadline, and infrastructure-loss scenarios.

use std::num::NonZeroUsize;

use kafka_driver_core::{
    AuthenticationFailure, EffectId, KafkaSessionAuthenticationState, KafkaSessionCloseReason,
    KafkaSessionState,
};

use crate::{
    ScramProofLimits,
    reactor::scram_proof::{ScramProofRequest, ScramProofWorker, proof_request},
};

use super::scram_fixture_test::{
    DEADLINE, NOW, ScramOwnerFixture, first_round, independent_pending,
};

const EFFECT: EffectId = EffectId::from_raw(3);

#[test]
fn exact_proof_completes_once_while_a_wrong_fence_is_ignored() {
    let mut fixture = ScramOwnerFixture::new();
    let (worker, requests, _outcomes) = ScramProofWorker::isolated(limits(2));
    fixture.owner.scram_proof_sender = Some(worker.sender());
    let pending = fixture.arm_first_proof();
    fixture
        .owner
        .dispatch_scram_proof(EFFECT, first_round(), pending, NOW)
        .unwrap_or_else(|error| panic!("dispatch exact direct proof: {error}"));
    let exact = requests
        .try_recv()
        .unwrap_or_else(|error| panic!("receive exact direct proof: {error}"));
    let fence = exact.fence();
    assert_eq!(
        fence.target().direct_connection(),
        Some(fixture.owner.connection)
    );
    assert_eq!(fence.effect_id(), EFFECT);
    assert_eq!(fence.round(), first_round());

    let wrong = ScramProofRequest::direct(
        fixture.owner.connection,
        EffectId::from_raw(99),
        first_round(),
        independent_pending(),
    )
    .finish();
    assert!(
        !fixture
            .owner
            .complete_scram_proof(wrong, NOW)
            .unwrap_or_else(|error| panic!("reject wrong direct proof: {error}"))
    );
    assert_eq!(fixture.owner.pending_scram_proof, Some(fence));

    assert!(
        fixture
            .owner
            .complete_scram_proof(exact.finish(), NOW)
            .unwrap_or_else(|error| panic!("complete exact direct proof: {error}"))
    );
    assert!(fixture.owner.pending_scram_proof.is_none());
    assert!(matches!(
        fixture.owner.session.state(),
        KafkaSessionState::Authenticating {
            authentication: KafkaSessionAuthenticationState::Exchanging { round, .. },
            ..
        } if round.get() == 2
    ));

    let duplicate = ScramProofRequest::direct(
        fixture.owner.connection,
        EFFECT,
        first_round(),
        independent_pending(),
    )
    .finish();
    assert!(
        !fixture
            .owner
            .complete_scram_proof(duplicate, NOW)
            .unwrap_or_else(|error| panic!("reject duplicate direct proof: {error}"))
    );
}

#[test]
fn authentication_deadline_wins_over_an_exact_late_proof() {
    let mut fixture = ScramOwnerFixture::new();
    let (worker, requests, _outcomes) = ScramProofWorker::isolated(limits(1));
    fixture.owner.scram_proof_sender = Some(worker.sender());
    let pending = fixture.arm_first_proof();
    fixture
        .owner
        .dispatch_scram_proof(EFFECT, first_round(), pending, NOW)
        .unwrap_or_else(|error| panic!("dispatch held direct proof: {error}"));
    let held = requests
        .try_recv()
        .unwrap_or_else(|error| panic!("receive held direct proof: {error}"));

    assert!(
        !fixture
            .owner
            .complete_scram_proof(held.finish(), DEADLINE)
            .unwrap_or_else(|error| panic!("reject late direct proof: {error}"))
    );
    assert_authentication_closed(
        &fixture,
        KafkaSessionCloseReason::AuthenticationFailed(AuthenticationFailure::Timeout),
    );
}

#[test]
fn shutdown_during_derivation_clears_proof_and_authentication_ownership() {
    let mut fixture = ScramOwnerFixture::new();
    let (worker, requests, _outcomes) = ScramProofWorker::isolated(limits(1));
    fixture.owner.scram_proof_sender = Some(worker.sender());
    let pending = fixture.arm_first_proof();
    fixture
        .owner
        .dispatch_scram_proof(EFFECT, first_round(), pending, NOW)
        .unwrap_or_else(|error| panic!("dispatch shutdown-held direct proof: {error}"));
    let held = requests
        .try_recv()
        .unwrap_or_else(|error| panic!("receive shutdown-held direct proof: {error}"));

    fixture
        .owner
        .begin_session_drain(NOW)
        .unwrap_or_else(|error| panic!("drain authenticating direct owner: {error}"));

    assert_authentication_closed(&fixture, KafkaSessionCloseReason::Requested);
    assert!(
        !fixture
            .owner
            .complete_scram_proof(held.finish(), NOW)
            .unwrap_or_else(|error| panic!("reject proof after direct shutdown: {error}"))
    );
    assert_authentication_closed(&fixture, KafkaSessionCloseReason::Requested);
}

#[test]
fn full_proof_queue_is_local_capacity_and_clears_secret_ownership() {
    let mut fixture = ScramOwnerFixture::new();
    let (worker, _requests, _outcomes) = ScramProofWorker::isolated(limits(1));
    let sender = worker.sender();
    sender
        .submit(proof_request(99))
        .unwrap_or_else(|error| panic!("occupy direct proof queue: {error}"));
    fixture.owner.scram_proof_sender = Some(sender);
    let pending = fixture.arm_first_proof();

    fixture
        .owner
        .dispatch_scram_proof(EFFECT, first_round(), pending, NOW)
        .unwrap_or_else(|error| panic!("settle full direct proof queue: {error}"));

    assert_authentication_closed(
        &fixture,
        KafkaSessionCloseReason::AuthenticationFailed(AuthenticationFailure::LocalCapacity),
    );
}

#[test]
fn closed_proof_worker_is_host_fatal_without_rewriting_session_state() {
    let mut fixture = ScramOwnerFixture::new();
    let (worker, requests, _outcomes) = ScramProofWorker::isolated(limits(1));
    fixture.owner.scram_proof_sender = Some(worker.sender());
    drop(requests);
    let pending = fixture.arm_first_proof();

    let error = fixture
        .owner
        .dispatch_scram_proof(EFFECT, first_round(), pending, NOW)
        .err()
        .unwrap_or_else(|| panic!("closed direct proof worker must fail the host"));

    assert_eq!(error.to_string(), "SCRAM proof worker was lost");
    assert!(fixture.owner.pending_scram_proof.is_none());
    assert!(fixture.owner.authentication_session.is_some());
    assert!(matches!(
        fixture.owner.session.state(),
        KafkaSessionState::Authenticating {
            authentication: KafkaSessionAuthenticationState::Exchanging { round, .. },
            ..
        } if round == first_round()
    ));
}

fn assert_authentication_closed(fixture: &ScramOwnerFixture, reason: KafkaSessionCloseReason) {
    assert_eq!(
        fixture.owner.session.state(),
        KafkaSessionState::Closing { reason }
    );
    assert!(fixture.owner.authentication_session.is_none());
    assert!(fixture.owner.pending_scram_proof.is_none());
    assert!(fixture.owner.scram_proof_sender.is_none());
    assert!(fixture.owner.session_deadline.is_none());
}

fn limits(capacity: usize) -> ScramProofLimits {
    let capacity = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN);
    ScramProofLimits::new(capacity, NonZeroUsize::MIN, NonZeroUsize::MIN)
}
