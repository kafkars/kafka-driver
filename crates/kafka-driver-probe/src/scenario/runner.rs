//! Scenario selection with unconditional graceful session closure.

use crate::{
    arguments::{Arguments, SaslSelection},
    endpoint,
    error::ProbeError,
    security,
    session::ProbeSession,
};

use super::{
    authentication, encryption, measurement, readiness, reconnect, routes, secure_authentication,
};

pub(crate) fn run(arguments: Arguments) -> Result<(), ProbeError> {
    let (session, scenario) = match arguments {
        Arguments::Readiness { bootstrap } => (spawn_plaintext(&bootstrap)?, Scenario::Readiness),
        Arguments::Routes {
            bootstrap,
            topic,
            group,
        } => (
            spawn_plaintext(&bootstrap)?,
            Scenario::Routes { topic, group },
        ),
        Arguments::Reconnect { bootstrap } => (spawn_plaintext(&bootstrap)?, Scenario::Reconnect),
        Arguments::Rolling {
            bootstrap,
            coordination,
        } => (
            spawn_plaintext(&bootstrap)?,
            Scenario::Rolling { coordination },
        ),
        Arguments::Authenticate {
            mechanism,
            bootstrap,
        } => {
            let endpoints = endpoint::bootstrap(&bootstrap)
                .map_err(|source| ProbeError::stage("validate bootstrap endpoint", source))?;
            (
                security::sasl_session(endpoints, mechanism)?,
                Scenario::Authentication { mechanism },
            )
        }
        Arguments::Tls {
            address,
            certificate,
            server_name,
        } => {
            let address = endpoint::socket(&address)
                .map_err(|source| ProbeError::stage("validate direct TLS endpoint", source))?;
            (
                security::tls_session(address, &certificate, server_name)?,
                Scenario::Tls,
            )
        }
        Arguments::TlsAuthenticate {
            mechanism,
            address,
            certificate,
            server_name,
        } => {
            let address = endpoint::socket(&address)
                .map_err(|source| ProbeError::stage("validate direct TLS endpoint", source))?;
            let sasl = security::sasl_config(mechanism)?;
            (
                security::tls_sasl_session(address, &certificate, server_name, sasl)?,
                Scenario::TlsAuthentication { mechanism },
            )
        }
        Arguments::Measure { bootstrap, samples } => {
            (spawn_plaintext(&bootstrap)?, Scenario::Measure { samples })
        }
    };
    let outcome = match scenario {
        Scenario::Readiness => readiness::run(&session),
        Scenario::Routes { topic, group } => routes::run(&session, topic, group),
        Scenario::Reconnect => reconnect::run(&session),
        Scenario::Rolling { coordination } => reconnect::run_rolling(&session, &coordination),
        Scenario::Authentication { mechanism } => authentication::run(&session, mechanism),
        Scenario::Tls => encryption::run(&session),
        Scenario::TlsAuthentication { mechanism } => {
            secure_authentication::run(&session, mechanism)
        }
        Scenario::Measure { samples } => measurement::run(&session, samples),
    };
    let close = session.close();
    outcome.and(close)
}

fn spawn_plaintext(bootstrap: &str) -> Result<ProbeSession, ProbeError> {
    let endpoints = endpoint::bootstrap(bootstrap)
        .map_err(|source| ProbeError::stage("validate bootstrap endpoint", source))?;
    ProbeSession::spawn(endpoints)
}

enum Scenario {
    Readiness,
    Routes { topic: String, group: String },
    Reconnect,
    Rolling { coordination: String },
    Authentication { mechanism: SaslSelection },
    Tls,
    TlsAuthentication { mechanism: SaslSelection },
    Measure { samples: std::num::NonZeroUsize },
}
