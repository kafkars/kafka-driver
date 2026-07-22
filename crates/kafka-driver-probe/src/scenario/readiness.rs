//! Smallest real-broker proof: await cluster ownership, then use the ready seed.

use crate::{error::ProbeError, session::ProbeSession};

pub(super) fn run(session: &ProbeSession) -> Result<(), ProbeError> {
    session.await_seed()?;
    println!("PASS any-broker ApiVersions");

    session.await_controller()?;
    println!("PASS controller readiness");
    Ok(())
}
