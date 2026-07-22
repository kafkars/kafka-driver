//! Scenario selection with unconditional graceful session closure.

use crate::{arguments::Arguments, endpoint, error::ProbeError, session::ProbeSession};

use super::{readiness, routes};

pub(crate) fn run(arguments: Arguments) -> Result<(), ProbeError> {
    let (bootstrap, scenario) = match arguments {
        Arguments::Readiness { bootstrap } => (bootstrap, Scenario::Readiness),
        Arguments::Routes {
            bootstrap,
            topic,
            group,
        } => (bootstrap, Scenario::Routes { topic, group }),
    };
    let endpoints = endpoint::bootstrap(&bootstrap)
        .map_err(|source| ProbeError::stage("validate bootstrap endpoint", source))?;
    let session = ProbeSession::spawn(endpoints)?;
    let outcome = match scenario {
        Scenario::Readiness => readiness::run(&session),
        Scenario::Routes { topic, group } => routes::run(&session, topic, group),
    };
    let close = session.close();
    outcome.and(close)
}

enum Scenario {
    Readiness,
    Routes { topic: String, group: String },
}
