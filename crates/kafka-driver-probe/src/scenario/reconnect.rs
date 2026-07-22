//! Same-session broker-loss observation followed by bounded recovery proof.

use std::{io, io::Write, path::Path, thread, time::Duration};

use kafka_driver::{CallFailure, RequestError};

use crate::{
    error::ProbeError,
    session::{ProbeSession, SeedObservation},
};

const OUTAGE_ATTEMPTS: usize = 120;
const OUTAGE_CALL_TIMEOUT: Duration = Duration::from_millis(500);
const OUTAGE_INTERVAL: Duration = Duration::from_millis(25);
const RECOVERY_ATTEMPTS: usize = 90;
const RECOVERY_CALL_TIMEOUT: Duration = Duration::from_secs(1);
const RECOVERY_INTERVAL: Duration = Duration::from_millis(100);
const COORDINATION_ATTEMPTS: usize = 600;
const COORDINATION_INTERVAL: Duration = Duration::from_millis(50);
const FIRST_STOP: &str = "broker-1-stopped";
const SECOND_STOP: &str = "broker-2-stopped";

pub(super) fn run(session: &ProbeSession) -> Result<(), ProbeError> {
    session.await_seed()?;
    announce("READY initial driver connection")?;

    await_outage(session)?;
    announce("OBSERVED broker outage")?;

    await_recovery(session)?;
    println!("PASS existing driver reconnected");
    Ok(())
}

pub(super) fn run_rolling(session: &ProbeSession, coordination: &str) -> Result<(), ProbeError> {
    session.await_seed()?;
    announce("READY initial multi-broker connection")?;

    await_gate(coordination, FIRST_STOP, "first rolling stop signal")?;
    await_recovery(session)?;
    announce("RECOVERED rolling broker failover 1")?;

    await_gate(coordination, SECOND_STOP, "second rolling stop signal")?;
    await_recovery(session)?;
    println!("PASS rolling broker failover 2");
    Ok(())
}

fn await_gate(coordination: &str, name: &str, label: &'static str) -> Result<(), ProbeError> {
    let gate = Path::new(coordination).join(name);
    for _ in 0..COORDINATION_ATTEMPTS {
        match gate.try_exists() {
            Ok(true) => return Ok(()),
            Ok(false) => thread::sleep(COORDINATION_INTERVAL),
            Err(source) => return Err(ProbeError::stage("observe rolling coordination", source)),
        }
    }
    Err(ProbeError::ReadinessAttempts {
        route: label,
        attempts: COORDINATION_ATTEMPTS,
    })
}

fn await_outage(session: &ProbeSession) -> Result<(), ProbeError> {
    for _ in 0..OUTAGE_ATTEMPTS {
        match session.observe_seed(OUTAGE_CALL_TIMEOUT)? {
            SeedObservation::Ready => thread::sleep(OUTAGE_INTERVAL),
            SeedObservation::Failed(error) if is_outage(&error) => return Ok(()),
            SeedObservation::Failed(error) => {
                return Err(ProbeError::stage("observe broker outage", error));
            }
        }
    }
    Err(ProbeError::ReadinessAttempts {
        route: "broker outage",
        attempts: OUTAGE_ATTEMPTS,
    })
}

fn await_recovery(session: &ProbeSession) -> Result<(), ProbeError> {
    for _ in 0..RECOVERY_ATTEMPTS {
        match session.observe_seed(RECOVERY_CALL_TIMEOUT)? {
            SeedObservation::Ready => return Ok(()),
            SeedObservation::Failed(error) if is_recovery_transient(&error) => {
                thread::sleep(RECOVERY_INTERVAL);
            }
            SeedObservation::Failed(error) => {
                return Err(ProbeError::stage("await same-driver recovery", error));
            }
        }
    }
    Err(ProbeError::ReadinessAttempts {
        route: "same-driver recovery",
        attempts: RECOVERY_ATTEMPTS,
    })
}

pub(super) fn is_outage(error: &RequestError) -> bool {
    matches!(
        error,
        RequestError::ConnectionClosed(_)
            | RequestError::Rejected {
                failure: CallFailure::NotReady
                    | CallFailure::Closed
                    | CallFailure::DeadlineExceeded
                    | CallFailure::ConnectionClosed { .. },
                ..
            }
    )
}

pub(super) fn is_recovery_transient(error: &RequestError) -> bool {
    is_outage(error) || matches!(error, RequestError::RouteUnavailable)
}

fn announce(message: &str) -> Result<(), ProbeError> {
    println!("{message}");
    io::stdout()
        .flush()
        .map_err(|source| ProbeError::stage("flush reconnect coordination", source))
}
