//! Context-rich failures that retain sources without exposing request bodies.

use std::{error::Error, fmt};

/// One failed qualification stage.
#[derive(Debug)]
pub(crate) enum ProbeError {
    Stage {
        stage: &'static str,
        source: Box<dyn Error>,
    },
    Kafka {
        route: &'static str,
        error_code: i16,
    },
    MissingApiVersions {
        route: &'static str,
    },
    ReadinessAttempts {
        route: &'static str,
        attempts: usize,
    },
    Invalidation {
        expected: kafka_driver::InvalidationDisposition,
        observed: kafka_driver::InvalidationDisposition,
    },
    Credential {
        name: &'static str,
    },
    AuthenticationAccepted,
    ReleaseRequired,
}

impl ProbeError {
    pub(crate) fn stage(stage: &'static str, source: impl Error + 'static) -> Self {
        Self::Stage {
            stage,
            source: Box::new(source),
        }
    }
}

impl fmt::Display for ProbeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stage { stage, source } => write!(formatter, "{stage}: {source}"),
            Self::Kafka { route, error_code } => {
                write!(formatter, "{route} returned Kafka error code {error_code}")
            }
            Self::MissingApiVersions { route } => {
                write!(
                    formatter,
                    "{route} omitted ApiVersions from its capability set"
                )
            }
            Self::ReadinessAttempts { route, attempts } => {
                write!(
                    formatter,
                    "{route} was not ready after {attempts} exact probes"
                )
            }
            Self::Invalidation { expected, observed } => {
                write!(
                    formatter,
                    "route invalidation disposition {observed:?} did not match {expected:?}"
                )
            }
            Self::Credential { name } => {
                write!(
                    formatter,
                    "credential environment variable {name} is missing or invalid"
                )
            }
            Self::AuthenticationAccepted => {
                formatter.write_str("invalid credentials were unexpectedly accepted")
            }
            Self::ReleaseRequired => {
                formatter.write_str("performance measurement requires a release build")
            }
        }
    }
}

impl Error for ProbeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Stage { source, .. } => Some(source.as_ref()),
            Self::Kafka { .. }
            | Self::MissingApiVersions { .. }
            | Self::ReadinessAttempts { .. }
            | Self::Invalidation { .. }
            | Self::Credential { .. }
            | Self::AuthenticationAccepted
            | Self::ReleaseRequired => None,
        }
    }
}
