//! Environment-owned SASL credentials reduced to the public driver configuration.

use std::env;

use kafka_driver::{BootstrapSet, SaslConfig};

use crate::{arguments::SaslSelection, error::ProbeError, session::ProbeSession};

const USERNAME: &str = "KAFKA_DRIVER_SASL_USERNAME";
const PASSWORD: &str = "KAFKA_DRIVER_SASL_PASSWORD";

pub(crate) fn session(
    endpoints: BootstrapSet,
    mechanism: SaslSelection,
) -> Result<ProbeSession, ProbeError> {
    let username = credential(USERNAME)?;
    let password = credential(PASSWORD)?;
    let config = match mechanism {
        SaslSelection::Plain => SaslConfig::plain(username, password),
        SaslSelection::ScramSha256 => SaslConfig::scram_sha_256(username, password),
        SaslSelection::ScramSha512 => SaslConfig::scram_sha_512(username, password),
    }
    .map_err(|source| ProbeError::stage("validate SASL credentials", source))?;
    ProbeSession::spawn_sasl(endpoints, config)
}

fn credential(name: &'static str) -> Result<String, ProbeError> {
    env::var_os(name)
        .and_then(|value| value.into_string().ok())
        .ok_or(ProbeError::Credential { name })
}
