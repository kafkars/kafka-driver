//! Strict command vocabulary for bounded qualification scenarios.

use std::{error::Error, fmt, num::NonZeroUsize};

const MAX_MEASUREMENT_SAMPLES: usize = 10_000;

/// One explicitly selected real-broker qualification scenario.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Arguments {
    /// Proves that the configured broker negotiates and answers a generated RPC.
    Readiness { bootstrap: String },

    /// Proves each semantic cluster route through a real broker.
    Routes {
        bootstrap: String,
        topic: String,
        group: String,
    },

    /// Proves one driver survives an externally orchestrated broker restart.
    Reconnect { bootstrap: String },

    /// Measures bounded generated-RPC progress through one real broker.
    Measure {
        bootstrap: String,
        samples: NonZeroUsize,
    },
}

impl Arguments {
    pub(crate) fn parse(values: impl IntoIterator<Item = String>) -> Result<Self, ArgumentError> {
        let values = values.into_iter().collect::<Vec<_>>();
        match values.as_slice() {
            [command, bootstrap] if command == "readiness" => Ok(Self::Readiness {
                bootstrap: bootstrap.clone(),
            }),
            [command, bootstrap, topic, group] if command == "routes" => Ok(Self::Routes {
                bootstrap: bootstrap.clone(),
                topic: topic.clone(),
                group: group.clone(),
            }),
            [command, bootstrap] if command == "reconnect" => Ok(Self::Reconnect {
                bootstrap: bootstrap.clone(),
            }),
            [command, bootstrap, samples] if command == "measure" => {
                let samples = samples
                    .parse::<usize>()
                    .ok()
                    .and_then(NonZeroUsize::new)
                    .filter(|samples| samples.get() <= MAX_MEASUREMENT_SAMPLES)
                    .ok_or(ArgumentError::Samples)?;
                Ok(Self::Measure {
                    bootstrap: bootstrap.clone(),
                    samples,
                })
            }
            _ => Err(ArgumentError::Shape),
        }
    }
}

/// The probe command did not match one complete scenario shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArgumentError {
    Shape,
    Samples,
}

impl fmt::Display for ArgumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shape => formatter.write_str(
                "usage: kafka-driver-probe readiness <host:port> | routes <host:port> <topic> <group> | reconnect <host:port> | measure <host:port> <samples>",
            ),
            Self::Samples => write!(
                formatter,
                "measurement samples must be between 1 and {MAX_MEASUREMENT_SAMPLES}"
            ),
        }
    }
}

impl Error for ArgumentError {}
