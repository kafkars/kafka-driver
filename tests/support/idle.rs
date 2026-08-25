//! Bounded embedded-host progression to an idle cross-thread wake baseline.

use std::time::Duration;

use kafka_driver::{Reactor, TurnOutcome};

pub(crate) fn settle_to_idle(reactor: &mut Reactor) {
    for _ in 0..32 {
        let outcome = reactor
            .turn(Duration::ZERO)
            .unwrap_or_else(|error| panic!("settle embedded reactor: {error}"));
        if matches!(outcome, TurnOutcome::Idle) {
            return;
        }
    }
    panic!("embedded reactor did not become idle within 32 bounded turns");
}
