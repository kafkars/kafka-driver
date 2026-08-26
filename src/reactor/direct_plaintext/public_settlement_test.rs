//! Exact close-reason ownership for outcomes transferred through recovery.

use std::{net::TcpListener, time::Duration};

use bornera_core::{
    CloseReason as BorneraCloseReason, Delivery as BorneraDelivery, OperationFailure,
    OperationOutcome,
};
use kafka_driver_core::{CallFailure, CallId, CloseReason, Delivery, Moment, TransportFailure};
use kafka_wire::ApiVersionsRequest;
use kafka_wire_core::ApiVersion;

use crate::{DriverLimits, RequestError, request::erased_request};

use super::owner::DirectPlaintextOwner;
use crate::reactor::causality::CausalSequence;

#[test]
fn recovered_outcome_uses_the_explicit_generation_reason() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind recovered-outcome broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read recovered-outcome address: {error}"));
    let mut owner = DirectPlaintextOwner::new(
        &DriverLimits::default(),
        address,
        None,
        None,
        Moment::ORIGIN,
    )
    .unwrap_or_else(|error| panic!("construct recovered-outcome owner: {error}"));
    let (call, request) = erased_request(
        CallId::from_raw(91),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );
    let preparation = request
        .prepare_bornera(
            ApiVersion::new(0),
            None,
            owner.outbound_limits,
            owner.decode_limits,
        )
        .unwrap_or_else(|error| panic!("prepare recovered public context: {error}"));
    let (_, context) = preparation.into_parts();
    let exact = CloseReason::TransportLost(TransportFailure::TimedOut);

    owner
        .access()
        .settle_public_outcome(
            context,
            OperationOutcome::Failed {
                failure: OperationFailure::ConnectionClosed(BorneraCloseReason::TransportLost),
                delivery: BorneraDelivery::PossiblySent,
            },
            Moment::ORIGIN,
            &mut CausalSequence::new(),
            false,
            Some(exact),
        )
        .unwrap_or_else(|error| panic!("settle recovered public outcome: {error}"));

    assert_eq!(
        call.try_result(),
        Some(Ok(Err(RequestError::Rejected {
            failure: CallFailure::ConnectionClosed { reason: exact },
            delivery: Delivery::PossiblySent,
        })))
    );
}
