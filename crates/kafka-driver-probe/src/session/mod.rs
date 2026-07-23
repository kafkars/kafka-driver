//! Dedicated-host lifetime and generated-RPC qualification facade.

mod api_versions;
#[cfg(test)]
mod api_versions_test;
mod observation;
mod owner;
mod tracked;

pub(crate) use api_versions::SeedObservation;
pub(crate) use owner::ProbeSession;
#[cfg(test)]
pub(crate) use tracked::movement_transient;
