//! Semantic readiness precedes fallible mechanical admission.

use std::{
    net::{SocketAddr, TcpListener},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use bornera::{ConnectionToken, OwnerFailure, TcpTransport};
use bornera_core::ConnectionEpoch as BorneraEpoch;
use kafka_driver_core::{BrokerState, CloseReason, ConnectionEpoch, Moment, TransportFailure};

use crate::DriverLimits;

use super::{
    attempt::{
        DirectConnectError, DirectConnectionAttempt, DirectConnectionOwner, PlaintextAttempt,
    },
    endpoint_selection_test::{recorded, resolved_lane},
    owner::DirectSet,
};
use crate::reactor::{broker::BrokerLimits, causality::CausalSequence};

const NOW: Moment = Moment::from_nanos(1);

#[test]
fn prior_retry_becoming_semantically_ready_resets_retry_before_admission_loss() {
    let first = listener();
    let second = listener();
    let first_address = address(&first);
    let second_address = address(&second);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let driver = DriverLimits::default();
    let attempt = FailFirstThenPlaintext {
        seen: Arc::clone(&seen),
        calls: AtomicUsize::new(0),
        delegate: PlaintextAttempt::new(&driver, BrokerLimits::default()),
    };
    let (mut set, mut lane) = resolved_lane([first_address, second_address], Box::new(attempt));
    let mut causality = CausalSequence::new();
    let first_deadline = backoff_deadline(&lane);
    set.access(&mut lane)
        .fire_due_reconnect(first_deadline, &mut causality)
        .unwrap_or_else(|error| panic!("open retry candidate: {error}"));
    assert_eq!(lane.connection_for_test().epoch(), BorneraEpoch::new(2));

    set.access(&mut lane)
        .mark_generation_ready(ConnectionEpoch::from_raw(2))
        .unwrap_or_else(|error| panic!("publish semantic readiness: {error}"));
    detach(&mut set.set, &mut lane);
    set.access(&mut lane)
        .settle_generation_lifecycle(
            ConnectionEpoch::from_raw(2),
            CloseReason::TransportLost(TransportFailure::Reset),
            NOW,
            &mut causality,
        )
        .unwrap_or_else(|error| panic!("settle admission loss: {error}"));
    let BrokerState::Backoff {
        retry, deadline, ..
    } = lane.lifecycle.state()
    else {
        panic!("admission loss must schedule a fresh retry");
    };
    assert_eq!(retry.get(), 1);
    set.access(&mut lane)
        .fire_due_reconnect(deadline, &mut causality)
        .unwrap_or_else(|error| panic!("retry semantically ready candidate: {error}"));

    assert_eq!(
        recorded(&seen),
        vec![first_address, second_address, second_address]
    );
    assert_eq!(lane.connection_for_test().epoch(), BorneraEpoch::new(3));
    assert!(!lane.endpoint_refresh_needed());
}

struct FailFirstThenPlaintext {
    seen: Arc<Mutex<Vec<SocketAddr>>>,
    calls: AtomicUsize,
    delegate: PlaintextAttempt,
}

impl DirectConnectionAttempt<TcpTransport> for FailFirstThenPlaintext {
    fn connect(
        &self,
        set: &mut DirectSet<TcpTransport>,
        owner: DirectConnectionOwner,
        address: SocketAddr,
        epoch: BorneraEpoch,
        now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        self.seen
            .lock()
            .unwrap_or_else(|error| panic!("record semantic-ready address: {error}"))
            .push(address);
        if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
            return Err(DirectConnectError::endpoint(
                std::io::ErrorKind::ConnectionRefused.into(),
            ));
        }
        self.delegate.connect(set, owner, address, epoch, now)
    }
}

fn detach(set: &mut DirectSet<TcpTransport>, lane: &mut super::owner::DirectLane<TcpTransport>) {
    let connection = lane.connection_for_test();
    drop(
        set.abandon(connection, OwnerFailure::OwnerInvariant)
            .unwrap_or_else(|error| panic!("detach semantic-ready generation: {error}")),
    );
    lane.connection = None;
}

fn backoff_deadline(lane: &super::owner::DirectLane<TcpTransport>) -> Moment {
    let BrokerState::Backoff { deadline, .. } = lane.lifecycle.state() else {
        panic!("failed candidate must enter backoff");
    };
    deadline
}

fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind semantic-ready candidate: {error}"))
}

fn address(listener: &TcpListener) -> SocketAddr {
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read semantic-ready candidate: {error}"))
}
