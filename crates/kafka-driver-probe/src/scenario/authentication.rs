//! Successful generated RPC proof after one selected real SASL exchange.

use crate::{arguments::SaslSelection, error::ProbeError, session::ProbeSession};

pub(super) fn run(session: &ProbeSession, mechanism: SaslSelection) -> Result<(), ProbeError> {
    session.await_seed()?;
    println!("PASS {mechanism} authentication");
    Ok(())
}
