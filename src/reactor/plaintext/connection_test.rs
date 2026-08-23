//! Loopback scenarios for bounded read, frame, and ordered write progress.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream as StandardStream},
    num::NonZeroUsize,
    time::Duration,
};

use kafka_driver_core::{CallId, EffectId};
use kafka_driver_transport::{FrameLimits, WriteQueueLimits};
use kafka_wire_core::Bytes;
use mio::net::TcpStream;

use super::connection::PlaintextConnection;
use crate::reactor::{
    PollEvent, Poller,
    poller::PollInterest,
    resource::ResourceToken,
    tcp::ConnectProgress,
    transport::{ReadBudget, ReadState, TransportLimits, WriteBudget, WriteState},
};

#[test]
fn read_byte_budget_retains_a_fragment_until_the_next_drive() {
    let (mut connection, mut peer) = connection_pair();
    assert!(peer.write_all(&framed(&[10, 20, 30])).is_ok());
    await_readable(&mut connection);
    let mut frames = Vec::new();

    let Ok(first) = connection.drive_read(ReadBudget::new(nonzero(5), nonzero(2)), &mut frames)
    else {
        panic!("fragment read must succeed");
    };

    assert_eq!(first.bytes(), 5);
    assert_eq!(first.frames(), 0);
    assert_eq!(first.state(), ReadState::BudgetExhausted);
    assert!(frames.is_empty());

    let Ok(second) = connection.drive_read(ReadBudget::new(nonzero(8), nonzero(2)), &mut frames)
    else {
        panic!("remaining read must succeed");
    };

    assert_eq!(second.bytes(), 2);
    assert_eq!(second.frames(), 1);
    assert_eq!(second.state(), ReadState::Blocked);
    assert_eq!(frames[0].as_bytes(), &[10, 20, 30]);
}

#[test]
fn frame_budget_retains_a_coalesced_frame_without_needing_new_readiness() {
    let (mut connection, mut peer) = connection_pair();
    let mut coalesced = framed(&[1]);
    coalesced.extend_from_slice(&framed(&[2]));
    assert!(peer.write_all(&coalesced).is_ok());
    await_readable(&mut connection);
    let mut frames = Vec::new();

    let Ok(first) = connection.drive_read(ReadBudget::new(nonzero(32), nonzero(1)), &mut frames)
    else {
        panic!("coalesced read must succeed");
    };
    assert_eq!(first.frames(), 1);
    assert_eq!(first.state(), ReadState::BudgetExhausted);
    assert_eq!(frames[0].as_bytes(), &[1]);

    let Ok(second) = connection.drive_read(ReadBudget::new(nonzero(32), nonzero(1)), &mut frames)
    else {
        panic!("buffered frame must succeed without another socket read");
    };
    assert_eq!(second.bytes(), 0);
    assert_eq!(second.frames(), 1);
    assert_eq!(second.state(), ReadState::BudgetExhausted);
    assert_eq!(frames[1].as_bytes(), &[2]);
}

#[test]
fn write_budget_preserves_exact_fifo_progress_across_drives() {
    let (mut connection, mut peer) = connection_pair();
    let frame = Bytes::from(framed(&[1, 2, 3]));
    assert!(
        connection
            .admit_write(call(1), effect(11), frame.clone())
            .is_ok()
    );
    let mut completed = Vec::new();

    let Ok(first) = connection.drive_write(WriteBudget::new(nonzero(3)), &mut completed) else {
        panic!("partial socket write must succeed");
    };

    assert_eq!(first.bytes(), 3);
    assert_eq!(first.completed(), 0);
    assert_eq!(first.state(), WriteState::BudgetExhausted);
    assert!(completed.is_empty());

    let Ok(second) = connection.drive_write(WriteBudget::new(nonzero(16)), &mut completed) else {
        panic!("remaining socket write must succeed");
    };
    assert_eq!(second.bytes(), frame.len() - 3);
    assert_eq!(second.completed(), 1);
    assert_eq!(second.state(), WriteState::Idle);
    assert_eq!(completed[0].call_id(), call(1));
    assert_eq!(completed[0].effect_id(), effect(11));
    assert_eq!(completed[0].frame_bytes(), frame.len());

    let mut observed = vec![0; frame.len()];
    assert!(peer.read_exact(&mut observed).is_ok());
    assert_eq!(observed, frame.as_ref());
}

#[test]
fn idle_socket_reports_blocked_without_consuming_a_budget() {
    let (mut connection, _peer) = connection_pair();
    let mut frames = Vec::new();

    let Ok(progress) = connection.drive_read(ReadBudget::new(nonzero(8), nonzero(1)), &mut frames)
    else {
        panic!("idle nonblocking read must succeed");
    };

    assert_eq!(progress.bytes(), 0);
    assert_eq!(progress.frames(), 0);
    assert_eq!(progress.state(), ReadState::Blocked);
    assert!(frames.is_empty());
    assert_eq!(connection.queued_write_frames(), 0);
}

#[test]
fn nonblocking_connect_opens_only_after_real_readiness_verification() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback listener: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback address: {error}"));
    let mut connection = PlaintextConnection::connect(address, limits())
        .unwrap_or_else(|error| panic!("start nonblocking connect: {error}"));
    let (_peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept loopback connection: {error}"));
    await_interest(&mut connection, PollInterest::READ_WRITE);

    let Ok(opened) = connection.finish_connect() else {
        panic!("ready connect must verify open");
    };
    let Ok(already_open) = connection.finish_connect() else {
        panic!("open connect verification must be idempotent");
    };
    assert_eq!(opened, ConnectProgress::Opened);
    assert_eq!(already_open, ConnectProgress::AlreadyOpen);
}

fn connection_pair() -> (PlaintextConnection, StandardStream) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback listener: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback address: {error}"));
    let client = StandardStream::connect(address)
        .unwrap_or_else(|error| panic!("connect loopback client: {error}"));
    let (peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept loopback client: {error}"));
    client
        .set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make client nonblocking: {error}"));
    let socket = TcpStream::from_std(client);
    (PlaintextConnection::new(socket, limits()), peer)
}

fn await_readable(connection: &mut PlaintextConnection) {
    await_interest(connection, PollInterest::READABLE);
}

fn await_interest(connection: &mut PlaintextConnection, interest: PollInterest) {
    let Ok(mut poller) = Poller::new(NonZeroUsize::MIN) else {
        panic!("host must provide a Mio selector");
    };
    let token = ResourceToken::new(
        calandria::ResourceOwnerId::new(0),
        calandria::ResourceSlotId::new(0),
        calandria::ResourceGeneration::INITIAL,
    );
    assert!(poller.register(connection, token, interest).is_ok());
    let mut events = Vec::with_capacity(1);
    let Ok(observed) = poller.poll_into(Some(Duration::from_secs(1)), &mut events) else {
        panic!("loopback readiness poll must succeed");
    };
    assert_eq!(observed, 1);
    let [
        PollEvent::Resource {
            token: observed,
            readiness,
        },
    ] = events.as_slice()
    else {
        panic!("one resource readiness event must be observed");
    };
    assert_eq!(*observed, token);
    if interest == PollInterest::READABLE {
        assert!(readiness.is_readable());
    } else {
        assert_eq!(interest, PollInterest::READ_WRITE);
        assert!(readiness.is_readable() || readiness.is_writable());
    }
    assert!(poller.deregister(connection, token).is_ok());
}

fn limits() -> TransportLimits {
    let Ok(frame) = FrameLimits::new(nonzero(60), nonzero(64)) else {
        panic!("test frame limits must be valid");
    };
    TransportLimits::new(
        frame,
        WriteQueueLimits::new(nonzero(4), nonzero(64)),
        nonzero(16),
    )
}

fn framed(body: &[u8]) -> Vec<u8> {
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("test body length must fit Kafka prefix: {error}"));
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(body);
    frame
}

fn call(raw: u64) -> CallId {
    CallId::from_raw(raw)
}

fn effect(raw: u64) -> EffectId {
    EffectId::from_raw(raw)
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test value must be nonzero"))
}
