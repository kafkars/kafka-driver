//! Release-only generated-RPC throughput and semantic-lane latency measurements.

use std::{num::NonZeroUsize, time::Instant};

use kafka_driver::{Route, TrafficClass};

use crate::{error::ProbeError, session::ProbeSession};

const PIPELINE_WIDTH: usize = 128;

pub(super) fn run(session: &ProbeSession, samples: NonZeroUsize) -> Result<(), ProbeError> {
    if cfg!(debug_assertions) {
        return Err(ProbeError::ReleaseRequired);
    }
    session.await_seed()?;
    session.await_controller()?;

    let sequential = sequential(session, samples.get())?;
    let pipelined = pipelined(session, samples.get())?;
    let control_under_bulk = control_under_bulk(session)?;
    println!(
        "{{\"schema\":1,\"samples\":{},\"pipeline_width\":{},\"sequential_ns\":{},\"sequential_rps\":{},\"pipelined_ns\":{},\"pipelined_rps\":{},\"control_under_bulk_ns\":{}}}",
        samples,
        PIPELINE_WIDTH,
        sequential,
        rate(samples.get(), sequential),
        pipelined,
        rate(samples.get(), pipelined),
        control_under_bulk,
    );
    Ok(())
}

fn sequential(session: &ProbeSession, samples: usize) -> Result<u128, ProbeError> {
    let started = Instant::now();
    for _ in 0..samples {
        let call = session.submit_api_versions(TrafficClass::Interactive, Route::AnyBroker)?;
        ProbeSession::complete_api_versions(call, "sequential any-broker route")?;
    }
    Ok(started.elapsed().as_nanos())
}

fn pipelined(session: &ProbeSession, samples: usize) -> Result<u128, ProbeError> {
    let started = Instant::now();
    let mut remaining = samples;
    while remaining != 0 {
        let width = remaining.min(PIPELINE_WIDTH);
        let mut calls = Vec::with_capacity(width);
        for _ in 0..width {
            calls.push(session.submit_api_versions(TrafficClass::Interactive, Route::AnyBroker)?);
        }
        for call in calls {
            ProbeSession::complete_api_versions(call, "pipelined any-broker route")?;
        }
        remaining -= width;
    }
    Ok(started.elapsed().as_nanos())
}

fn control_under_bulk(session: &ProbeSession) -> Result<u128, ProbeError> {
    let controller = Route::Controller;
    session.await_route_in(TrafficClass::Bulk, &controller, "bulk controller route")?;
    session.await_route_in(
        TrafficClass::Control,
        &controller,
        "control controller route",
    )?;

    let mut bulk = Vec::with_capacity(PIPELINE_WIDTH);
    for _ in 0..PIPELINE_WIDTH {
        bulk.push(session.submit_api_versions(TrafficClass::Bulk, controller.clone())?);
    }
    let started = Instant::now();
    let control = session.submit_api_versions(TrafficClass::Control, controller)?;
    ProbeSession::complete_api_versions(control, "control route under bulk load")?;
    let elapsed = started.elapsed().as_nanos();
    for call in bulk {
        ProbeSession::complete_api_versions(call, "bulk controller route")?;
    }
    Ok(elapsed)
}

fn rate(samples: usize, elapsed_ns: u128) -> u128 {
    (samples as u128 * 1_000_000_000) / elapsed_ns.max(1)
}
