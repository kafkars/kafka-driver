//! Strict command vocabulary for bounded qualification scenarios.

use std::{error::Error, fmt, num::NonZeroUsize};

const MAX_MEASUREMENT_SAMPLES: usize = 10_000;

/// One explicitly selected real-broker qualification scenario.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Arguments {
    /// Proves that the configured broker negotiates and answers a generated RPC.
    Readiness { bootstrap: String },

    /// Proves one logical endpoint advances past a refused DNS candidate.
    DnsRotation { bootstrap: String },

    /// Proves each semantic cluster route through a real broker.
    Routes {
        bootstrap: String,
        topic: String,
        group: String,
    },

    /// Proves one driver survives an externally orchestrated broker restart.
    Reconnect { bootstrap: String },

    /// Proves one driver survives two ordered broker losses in a live cluster.
    Rolling {
        bootstrap: String,
        coordination: String,
    },

    /// Proves ordered broker loss through endpoint-derived TLS identities.
    TlsRolling {
        bootstrap: String,
        certificate: String,
        coordination: String,
    },

    /// Proves one advertised partition broker moves within the same driver.
    Movement {
        bootstrap: String,
        topic: String,
        coordination: String,
    },

    /// Proves one exact SASL mechanism against an authenticated broker.
    Authenticate {
        mechanism: SaslSelection,
        bootstrap: String,
    },

    /// Proves one exact SASL mechanism rejects invalid credentials terminally.
    RejectAuthentication {
        mechanism: SaslSelection,
        bootstrap: String,
    },

    /// Proves a direct broker RPC over certificate-verified rustls.
    Tls {
        address: String,
        certificate: String,
        server_name: String,
    },

    /// Proves one SASL mechanism over certificate-verified rustls.
    TlsAuthenticate {
        mechanism: SaslSelection,
        address: String,
        certificate: String,
        server_name: String,
    },

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
            [command, bootstrap] if command == "dns-rotation" => Ok(Self::DnsRotation {
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
            [command, bootstrap, coordination] if command == "rolling" => Ok(Self::Rolling {
                bootstrap: bootstrap.clone(),
                coordination: coordination.clone(),
            }),
            [command, bootstrap, certificate, coordination] if command == "tls-rolling" => {
                Ok(Self::TlsRolling {
                    bootstrap: bootstrap.clone(),
                    certificate: certificate.clone(),
                    coordination: coordination.clone(),
                })
            }
            [command, bootstrap, topic, coordination] if command == "movement" => {
                Ok(Self::Movement {
                    bootstrap: bootstrap.clone(),
                    topic: topic.clone(),
                    coordination: coordination.clone(),
                })
            }
            [command, mechanism, bootstrap] if command == "authenticate" => {
                Ok(Self::Authenticate {
                    mechanism: SaslSelection::parse(mechanism)?,
                    bootstrap: bootstrap.clone(),
                })
            }
            [command, mechanism, bootstrap] if command == "reject-authentication" => {
                Ok(Self::RejectAuthentication {
                    mechanism: SaslSelection::parse(mechanism)?,
                    bootstrap: bootstrap.clone(),
                })
            }
            [command, address, certificate, server_name] if command == "tls" => Ok(Self::Tls {
                address: address.clone(),
                certificate: certificate.clone(),
                server_name: server_name.clone(),
            }),
            [command, mechanism, address, certificate, server_name]
                if command == "tls-authenticate" =>
            {
                Ok(Self::TlsAuthenticate {
                    mechanism: SaslSelection::parse(mechanism)?,
                    address: address.clone(),
                    certificate: certificate.clone(),
                    server_name: server_name.clone(),
                })
            }
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
    SaslMechanism,
}

impl fmt::Display for ArgumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shape => formatter.write_str(
                "usage: kafka-driver-probe readiness <bootstrap-set> | dns-rotation <bootstrap-set> | routes <bootstrap-set> <topic> <group> | reconnect <bootstrap-set> | rolling <bootstrap-set> <coordination-directory> | tls-rolling <bootstrap-set> <ca.pem> <coordination-directory> | movement <bootstrap-set> <topic> <coordination-directory> | authenticate <mechanism> <bootstrap-set> | reject-authentication <mechanism> <bootstrap-set> | tls <ip:port> <ca.pem> <server-name> | tls-authenticate <mechanism> <ip:port> <ca.pem> <server-name> | measure <bootstrap-set> <samples>",
            ),
            Self::Samples => write!(
                formatter,
                "measurement samples must be between 1 and {MAX_MEASUREMENT_SAMPLES}"
            ),
            Self::SaslMechanism => formatter.write_str(
                "SASL mechanism must be plain, scram-sha-256, or scram-sha-512",
            ),
        }
    }
}

impl Error for ArgumentError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SaslSelection {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl SaslSelection {
    fn parse(value: &str) -> Result<Self, ArgumentError> {
        match value {
            "plain" => Ok(Self::Plain),
            "scram-sha-256" => Ok(Self::ScramSha256),
            "scram-sha-512" => Ok(Self::ScramSha512),
            _ => Err(ArgumentError::SaslMechanism),
        }
    }
}

impl fmt::Display for SaslSelection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Plain => "SASL PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        })
    }
}
