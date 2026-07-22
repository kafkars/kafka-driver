//! Curated real-broker scenarios over only the public driver surface.

mod authentication;
mod authentication_rejection;
#[cfg(test)]
mod authentication_rejection_test;
mod encryption;
mod measurement;
mod movement;
#[cfg(test)]
mod movement_test;
mod readiness;
mod reconnect;
#[cfg(test)]
mod reconnect_test;
mod routes;
mod runner;
mod secure_authentication;

pub(crate) use runner::run;
