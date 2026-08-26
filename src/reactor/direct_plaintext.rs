//! Direct numeric broker ownership on one Bornera selector.

mod admission;
mod attempt;
mod authentication_admission;
mod authentication_publication;
mod authentication_reserve;
mod authentication_settlement;
mod backend;
mod construction;
mod decoder_gate;
mod drive;
#[allow(
    dead_code,
    reason = "endpoint-refresh ownership is consumed by the pending bootstrap cutover"
)]
mod endpoint_refresh;
mod failure_translation;
mod generation;
mod invariant_failure;
mod lane_construction;
mod lifecycle;
mod limits;
mod negotiation;
mod observation;
mod operation_owner;
mod owner;
mod pending;
mod public_settlement;
mod publication;
mod reconnect;
mod recovery_owners;
mod recovery_settlement;
mod runtime;
#[cfg(feature = "tls-rustls")]
mod rustls_transport;
mod scram_proof;
mod session_close;
mod session_plan;
mod session_progress;
mod set_drive;
mod set_owner;
mod set_schedule;
mod settlement;

#[cfg(test)]
mod attempt_test;
#[cfg(test)]
mod authentication_fixture_test;
#[cfg(test)]
mod authentication_publication_test;
#[cfg(test)]
mod authentication_test;
#[cfg(test)]
#[cfg(feature = "tls-rustls")]
mod decoder_gate_test;
#[cfg(test)]
mod drive_test;
#[cfg(test)]
mod endpoint_failure_policy_test;
#[cfg(test)]
mod endpoint_selection_test;
#[cfg(test)]
mod lifecycle_test;
#[cfg(test)]
mod owner_test;
#[cfg(test)]
mod pending_test;
#[cfg(test)]
mod public_settlement_test;
#[cfg(test)]
mod reconnect_edge_test;
#[cfg(test)]
mod reconnect_fatal_test;
#[cfg(test)]
mod recovery_test;
#[cfg(test)]
mod recovery_totality_test;
#[cfg(test)]
mod resolved_recovery_test;
#[cfg(all(test, feature = "tls-rustls"))]
mod rustls_attempt_test;
#[cfg(test)]
pub(in crate::reactor) mod scram_fixture_test;
#[cfg(test)]
mod scram_proof_test;
#[cfg(test)]
mod semantic_readiness_test;
#[cfg(test)]
mod shared_set_failure_test;
#[cfg(test)]
mod shared_set_fixture_test;
#[cfg(test)]
mod shared_set_test;

pub(in crate::reactor) use backend::DirectBackend;
pub(in crate::reactor) use endpoint_refresh::DirectEndpointRefresh;
