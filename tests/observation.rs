//! Public bounded operational-snapshot scenario over the embedded host.

use std::{net::TcpListener, time::Duration};

use kafka_driver::{Driver, TurnOutcome};

#[test]
fn snapshot_reports_seed_state_and_post_drain_mailbox_pressure() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind snapshot broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read snapshot broker address: {error}"));
    let (driver, mut reactor) = Driver::builder()
        .broker(address)
        .build_reactor()
        .unwrap_or_else(|error| panic!("build direct snapshot reactor: {error}"));
    let call = driver
        .snapshot()
        .unwrap_or_else(|error| panic!("admit snapshot command: {error}"));

    let turn = reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("interpret snapshot command: {error}"));
    let snapshot = call
        .wait()
        .unwrap_or_else(|error| panic!("observe snapshot completion: {error}"))
        .unwrap_or_else(|error| panic!("snapshot rejected: {error}"));

    assert!(matches!(turn, TurnOutcome::Progress { commands: 1, .. }));
    assert_eq!(snapshot.mailbox().queued_work(), 0);
    assert_eq!(snapshot.mailbox().queued_control(), 0);
    assert!(snapshot.seed().is_some());
    assert!(snapshot.metadata_generation().is_none());
    assert!(snapshot.lanes().is_empty());
}
