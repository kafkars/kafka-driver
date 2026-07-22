//! Smallest real-broker proof: negotiate and complete one generated RPC.

use kafka_driver::Route;

use crate::{error::ProbeError, session::ProbeSession};

pub(super) fn run(session: &ProbeSession) -> Result<(), ProbeError> {
    session.api_versions(Route::AnyBroker, "any-broker route")?;
    println!("PASS any-broker ApiVersions");
    Ok(())
}
