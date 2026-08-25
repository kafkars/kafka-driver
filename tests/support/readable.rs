//! Bounded embedded-host progression until a loopback request reaches the peer.

use std::{io::ErrorKind, net::TcpStream, time::Duration};

use kafka_driver::Reactor;

pub(crate) fn drive_until_readable(peer: &TcpStream, reactor: &mut Reactor) {
    peer.set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make loopback broker nonblocking: {error}"));
    let mut probe = [0; 1];
    for _ in 0..8 {
        match peer.peek(&mut probe) {
            Ok(1..) => {
                restore_blocking(peer);
                return;
            }
            Ok(0) => break,
            Err(error) if error.kind() == ErrorKind::WouldBlock => {}
            Err(error) => {
                restore_blocking(peer);
                panic!("peek loopback broker: {error}");
            }
        }
        reactor
            .turn(Duration::from_millis(100))
            .unwrap_or_else(|error| panic!("drive loopback request publication: {error}"));
    }
    restore_blocking(peer);
    panic!("loopback request did not become readable within eight bounded turns");
}

fn restore_blocking(peer: &TcpStream) {
    peer.set_nonblocking(false)
        .unwrap_or_else(|error| panic!("restore loopback broker blocking: {error}"));
}
