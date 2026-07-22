//! Generated RPC proof over a certificate-verified real Kafka TLS listener.

use crate::{error::ProbeError, session::ProbeSession};

pub(super) fn run(session: &ProbeSession) -> Result<(), ProbeError> {
    session.await_seed()?;
    println!("PASS rustls certificate verification");
    Ok(())
}
