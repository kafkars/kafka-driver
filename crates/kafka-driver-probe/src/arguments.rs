//! Strict command vocabulary for bounded qualification scenarios.

use std::{error::Error, fmt};

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
            _ => Err(ArgumentError),
        }
    }
}

/// The probe command did not match one complete scenario shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ArgumentError;

impl fmt::Display for ArgumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "usage: kafka-driver-probe readiness <host:port> | routes <host:port> <topic> <group>",
        )
    }
}

impl Error for ArgumentError {}
