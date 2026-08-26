//! Recovered readiness must preserve the last admitted resolved candidate.

use std::{
    net::{SocketAddr, TcpListener},
    sync::{Arc, Mutex},
    time::Duration,
};

use bornera::{ConnectionEvent, OwnerFailure, TransportState};
use bornera_core::CloseReason as BorneraCloseReason;
use calandria::Span;
use kafka_driver_core::{BrokerState, ConnectionEpoch, Moment};

use crate::DriverLimits;

use super::{
    attempt::PlaintextAttempt,
    endpoint_selection_test::{RecordingPlaintext, recorded, resolved_lane},
    owner::{DirectLane, calandria_moment},
    set_owner::DirectSetOwner,
};
use crate::reactor::{broker::BrokerLimits, causality::CausalSequence};

const NOW: Moment = Moment::from_nanos(1);

#[test]
fn recovered_admission_and_close_retry_the_admitted_resolved_candidate() {
    let first = listener();
    let second = listener();
    let first_address = address(&first);
    let second_address = address(&second);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let driver = DriverLimits::default();
    let attempt = RecordingPlaintext {
        seen: Arc::clone(&seen),
        delegate: PlaintextAttempt::new(&driver, BrokerLimits::default()),
    };
    let (mut set, mut lane) = resolved_lane([first_address, second_address], Box::new(attempt));
    drive_transport_open(&mut set, &lane);
    let connection = lane.connection_for_test();
    set.set
        .open_admission(connection)
        .unwrap_or_else(|error| panic!("publish recovered admission: {error}"));
    set.set
        .finalize(connection, BorneraCloseReason::TransportLost)
        .unwrap_or_else(|error| panic!("publish recovered close: {error}"));
    let report = set
        .set
        .abandon(connection, OwnerFailure::OwnerInvariant)
        .unwrap_or_else(|error| panic!("recover admitted resolved generation: {error}"));
    assert!(report.events.iter().any(|event| matches!(
        event,
        ConnectionEvent::AdmissionOpened { epoch, .. } if *epoch == connection.epoch()
    )));
    assert!(report.events.iter().any(|event| matches!(
        event,
        ConnectionEvent::Closed { epoch, .. } if *epoch == connection.epoch()
    )));

    let mut causality = CausalSequence::new();
    set.access(&mut lane).capture_recovery(report);
    assert!(
        set.access(&mut lane)
            .settle_pending_recovery(NOW, &mut causality)
            .unwrap_or_else(|error| panic!("settle resolved recovery report: {error}"))
    );
    let BrokerState::Backoff { deadline, .. } = lane.lifecycle.state() else {
        panic!("recovered close must schedule reconnect");
    };
    set.access(&mut lane)
        .fire_due_reconnect(deadline, &mut causality)
        .unwrap_or_else(|error| panic!("open recovered resolved generation: {error}"));

    assert_eq!(recorded(&seen), vec![first_address, first_address]);
    assert_eq!(
        lane.connection_for_test().epoch(),
        bornera_core::ConnectionEpoch::new(2)
    );
    assert!(matches!(
        lane.lifecycle.state(),
        BrokerState::Connecting { epoch, .. } if epoch == ConnectionEpoch::from_raw(2)
    ));
}

fn drive_transport_open(
    set: &mut DirectSetOwner<bornera::TcpTransport>,
    lane: &DirectLane<bornera::TcpTransport>,
) {
    for _ in 0..32 {
        let connection = lane.connection_for_test();
        set.set
            .turn_component(calandria_moment(NOW))
            .unwrap_or_else(|error| panic!("drive resolved transport: {error}"));
        if set
            .set
            .connection_snapshot(connection)
            .is_ok_and(|snapshot| snapshot.transport == TransportState::Open)
        {
            return;
        }
        set.set
            .poll_io(Span::try_from(Duration::from_millis(50)).unwrap_or(Span::ZERO))
            .unwrap_or_else(|error| panic!("wait for resolved transport: {error}"));
    }
    panic!("resolved transport did not open");
}

fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind recovery candidate: {error}"))
}

fn address(listener: &TcpListener) -> SocketAddr {
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read recovery candidate: {error}"))
}
