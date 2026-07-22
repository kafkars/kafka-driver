//! Successful generated RPC proof after SASL over certificate-verified rustls.

use crate::{arguments::SaslSelection, error::ProbeError, session::ProbeSession};

pub(super) fn run(session: &ProbeSession, mechanism: SaslSelection) -> Result<(), ProbeError> {
    session.await_seed()?;
    println!("PASS {mechanism} over rustls certificate verification");
    Ok(())
}
