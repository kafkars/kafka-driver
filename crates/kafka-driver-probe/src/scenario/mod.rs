//! Curated real-broker scenarios over only the public driver surface.

mod readiness;
mod routes;
mod runner;

pub(crate) use runner::run;
