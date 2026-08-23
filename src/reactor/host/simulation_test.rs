//! Production-duty proof under Calandria's deterministic scheduler and replay.

use std::num::NonZeroUsize;

use calandria::{RetainedBytes, Span};
use calandria_sim::{
    EntropySeed, EntropyStreamId, RunEnd, Seeded, Simulation, SimulationLimits, TimelineId,
    Topology, Trace, TraceLimits,
};

use super::simulation_model_test::{CapabilityEvent, DRIVER_DUTY, DriverWorld};

const REPLAY_SEED: u64 = 781_199;

#[test]
fn real_driver_duty_replays_bootstrap_negotiation_metadata_and_shutdown() {
    let mut original = simulation(Seeded::new(
        EntropySeed::new(REPLAY_SEED),
        EntropyStreamId::new(1),
    ));
    connect(&mut original, "connection event");
    let report = original
        .run_to_completion()
        .unwrap_or_else(|error| panic!("driver simulation must complete: {error:?}"));
    assert_eq!(report.end(), RunEnd::Completed);
    original.model().assert_complete();

    let replay = original.monitor().replay();
    let mut repeated = simulation(replay);
    connect(&mut repeated, "replay connection event");
    repeated
        .run_to_completion()
        .unwrap_or_else(|error| panic!("driver replay must complete: {error:?}"));
    assert!(repeated.scheduler().is_complete());
    repeated.model().assert_complete();
}

fn simulation<S>(scheduler: S) -> Simulation<DriverWorld, S, Trace>
where
    S: calandria_sim::Scheduler,
{
    let topology = Topology::new([DRIVER_DUTY])
        .unwrap_or_else(|error| panic!("driver topology must be valid: {error}"));
    Simulation::with_scheduler(
        TimelineId::new(1),
        DriverWorld::new(),
        topology,
        SimulationLimits::default(),
        scheduler,
    )
    .unwrap_or_else(|error| panic!("driver simulation must build: {error}"))
    .with_monitor(Trace::new(TraceLimits::new(
        NonZeroUsize::new(64).unwrap_or_else(|| panic!("trace capacity must be nonzero")),
        RetainedBytes::ZERO,
    )))
}

fn connect<S>(simulation: &mut Simulation<DriverWorld, S, Trace>, phase: &str)
where
    S: calandria_sim::Scheduler,
{
    simulation
        .inject_after(DRIVER_DUTY, Span::from_nanos(1), CapabilityEvent::Connected)
        .unwrap_or_else(|error| panic!("{phase} must fit: {error}"));
}
