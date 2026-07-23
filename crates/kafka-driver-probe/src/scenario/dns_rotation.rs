//! Real-broker proof that one logical endpoint advances past a refused address.

use kafka_driver::{ConnectionCloseReason, TransportFailure};

use crate::{error::ProbeError, session::ProbeSession};

pub(super) fn run(session: &ProbeSession) -> Result<(), ProbeError> {
    session.await_seed()?;
    let snapshot = session.snapshot()?;
    let observed = snapshot.seed().and_then(|seed| seed.last_close_reason());
    require_refused_candidate(observed)?;
    println!("PASS dead-first DNS address rotation");
    Ok(())
}

pub(super) fn require_refused_candidate(
    observed: Option<ConnectionCloseReason>,
) -> Result<(), ProbeError> {
    let expected = ConnectionCloseReason::OpenFailed(TransportFailure::Refused);
    if observed == Some(expected) {
        return Ok(());
    }
    Err(ProbeError::AddressRotation { observed })
}
