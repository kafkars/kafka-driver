//! Host-fatal direct reconnect resource-exhaustion proof.

use std::{
    io,
    net::{SocketAddr, TcpListener},
    time::Duration,
};

use bornera::{ConnectionToken, TcpTransport};
use bornera_core::ConnectionEpoch as BorneraEpoch;
use kafka_driver_core::{
    BrokerCloseReason, BrokerState, CallFailure, CallId, CloseReason, ConnectionEpoch, Delivery,
    Moment, TransportFailure,
};
use kafka_wire::ApiVersionsRequest;

use crate::{DriverLimits, RequestError, request::erased_request};

use super::{
    attempt::{DirectConnectError, DirectConnectionAttempt, DirectConnectionOwner},
    owner::{DirectPlaintextOwner, DirectSet},
};
use crate::reactor::causality::CausalSequence;

const NOW: Moment = Moment::from_nanos(1);

#[test]
fn initial_endpoint_failure_can_close_policy_without_failing_construction() {
    let address = SocketAddr::from(([127, 0, 0, 1], 9));
    let mut owner = DirectPlaintextOwner::new_with_attempt(
        &DriverLimits::default(),
        address,
        None,
        Box::new(ImmediateEndpointFailure),
        Moment::from_nanos(u64::MAX),
    )
    .unwrap_or_else(|error| panic!("construct initially policy-closed owner: {error}"));

    assert!(matches!(
        owner.lane.lifecycle.state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::ClockOverflow
        }
    ));
    assert!(owner.lane.connection.is_none());
    assert_eq!(owner.selector_registrations(), 0);
    assert!(owner.is_terminal());
    let (call, request) = erased_request(
        CallId::from_raw(84),
        ApiVersionsRequest::default(),
        Duration::from_secs(5),
    );
    owner
        .submit(request, NOW, &mut CausalSequence::new())
        .unwrap_or_else(|error| panic!("reject initially policy-closed call: {error}"));
    assert_eq!(
        call.try_result(),
        Some(Ok(Err(RequestError::Rejected {
            failure: CallFailure::Closed,
            delivery: Delivery::NotSent,
        })))
    );
}

#[test]
fn timer_identity_exhaustion_is_repeatable_host_fatal_without_policy_close() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind timer-exhaustion listener: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read timer-exhaustion address: {error}"));
    let mut owner = DirectPlaintextOwner::new(&DriverLimits::default(), address, None, None, NOW)
        .unwrap_or_else(|error| panic!("construct timer-exhaustion owner: {error}"));
    let (pending, request) = erased_request(
        CallId::from_raw(85),
        ApiVersionsRequest::default(),
        Duration::from_secs(5),
    );
    let mut causality = CausalSequence::new();
    owner
        .submit(request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("queue before timer exhaustion: {error}"));
    assert!(pending.try_result().is_none());
    let connection = owner.lane.connection_for_test();
    drop(
        owner
            .connections
            .set
            .abandon(connection, bornera::OwnerFailure::OwnerInvariant)
            .unwrap_or_else(|error| panic!("detach timer-exhaustion connection: {error}")),
    );
    owner.lane.connection = None;
    owner.lane.lifecycle.exhaust_timer_ids();

    for _ in 0..2 {
        let error = owner
            .access()
            .settle_generation_lifecycle(
                ConnectionEpoch::from_raw(1),
                CloseReason::OpenFailed(TransportFailure::Refused),
                NOW,
                &mut causality,
            )
            .err()
            .unwrap_or_else(|| panic!("timer exhaustion must fail the host"));
        assert_eq!(
            error.to_string(),
            "direct reconnect timer identities were exhausted"
        );
        assert!(matches!(
            owner.lane.lifecycle.state(),
            BrokerState::Connecting { epoch, .. } if epoch == ConnectionEpoch::from_raw(1)
        ));
        assert!(owner.is_terminal());
        assert!(owner.seed_snapshot().is_none());
    }
    assert!(owner.lane.pending.is_empty());
    assert!(matches!(pending.try_result(), Some(Ok(Err(_)))));
}

struct ImmediateEndpointFailure;

impl DirectConnectionAttempt<TcpTransport> for ImmediateEndpointFailure {
    fn connect(
        &self,
        _set: &mut DirectSet<TcpTransport>,
        _owner: DirectConnectionOwner,
        _epoch: BorneraEpoch,
        _now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        Err(DirectConnectError::endpoint(
            io::ErrorKind::ConnectionRefused.into(),
        ))
    }
}
