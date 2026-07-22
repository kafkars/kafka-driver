//! Dedicated-host lifetime and generated-RPC qualification facade.

mod api_versions;
mod owner;

pub(crate) use api_versions::SeedObservation;
pub(crate) use owner::ProbeSession;
