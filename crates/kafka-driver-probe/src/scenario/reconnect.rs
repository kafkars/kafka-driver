//! Same-session broker-loss observation followed by bounded recovery proof.

use std::{io, io::Write, thread, time::Duration};

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

pub(super) fn run(session: &ProbeSession) -> Result<(), ProbeError> {
    session.await_seed()?;
    announce("READY initial driver connection")?;

    await_outage(session)?;
    announce("OBSERVED broker outage")?;

    await_recovery(session)?;
    println!("PASS existing driver reconnected");
    Ok(())
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
