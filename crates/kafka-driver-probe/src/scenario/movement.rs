//! Same-session advertised partition-broker movement through opaque route tokens.

use std::{io, io::Write, path::Path, thread, time::Duration};

use kafka_driver::{InvalidationDisposition, PartitionId, Route, RouteKind, TopicName};

use crate::{error::ProbeError, session::ProbeSession};

const GATE: &str = "broker-moved";
const GATE_ATTEMPTS: usize = 1_200;
const GATE_INTERVAL: Duration = Duration::from_millis(50);

pub(super) fn run(
    session: &ProbeSession,
    topic: String,
    coordination: &str,
) -> Result<(), ProbeError> {
    let topic = TopicName::new(topic)
        .map_err(|source| ProbeError::stage("validate movement topic", source))?;
    let partition = PartitionId::new(0)
        .map_err(|source| ProbeError::stage("validate movement partition", source))?;
    let route = Route::PartitionLeader { topic, partition };
    let old_for_refresh =
        session.await_tracked_route(&route, "initial advertised partition route")?;
    let old_for_stale =
        session.await_tracked_route(&route, "second initial advertised partition route")?;
    announce("READY initial advertised broker route")?;

    await_gate(coordination)?;
    session.invalidate_route(old_for_refresh, InvalidationDisposition::Applied)?;
    let current = session.await_tracked_route(&route, "moved advertised partition route")?;
    if current.kind() != RouteKind::PartitionLeader {
        return Err(ProbeError::stage(
            "observe moved route kind",
            io::Error::other("moved endpoint issued a non-partition route token"),
        ));
    }
    session.invalidate_route(old_for_stale, InvalidationDisposition::IgnoredStale)?;
    println!("PASS advertised broker movement");
    Ok(())
}

fn await_gate(coordination: &str) -> Result<(), ProbeError> {
    let gate = Path::new(coordination).join(GATE);
    for _ in 0..GATE_ATTEMPTS {
        match gate.try_exists() {
            Ok(true) => return Ok(()),
            Ok(false) => thread::sleep(GATE_INTERVAL),
            Err(source) => return Err(ProbeError::stage("observe movement coordination", source)),
        }
    }
    Err(ProbeError::ReadinessAttempts {
        route: "broker movement signal",
        attempts: GATE_ATTEMPTS,
    })
}

fn announce(message: &str) -> Result<(), ProbeError> {
    println!("{message}");
    io::stdout()
        .flush()
        .map_err(|source| ProbeError::stage("flush movement coordination", source))
}
