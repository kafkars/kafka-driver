//! Real-cluster proof of controller, coordinator, and partition ownership discovery.

use kafka_driver::{CoordinatorKey, CoordinatorKind, PartitionId, Route, TopicName};

use crate::{error::ProbeError, session::ProbeSession};

pub(super) fn run(session: &ProbeSession, topic: String, group: String) -> Result<(), ProbeError> {
    session.await_seed()?;
    println!("PASS any-broker route");

    session.await_controller()?;
    println!("PASS controller route");

    let key = CoordinatorKey::new(CoordinatorKind::Group, group)
        .map_err(|source| ProbeError::stage("validate group coordinator key", source))?;
    session.await_route(&Route::Coordinator { key }, "group-coordinator route")?;
    println!("PASS group-coordinator route");

    let topic = TopicName::new(topic)
        .map_err(|source| ProbeError::stage("validate partition topic", source))?;
    let partition = PartitionId::new(0)
        .map_err(|source| ProbeError::stage("validate partition identity", source))?;
    session.await_route(
        &Route::PartitionLeader { topic, partition },
        "partition-leader route",
    )?;
    println!("PASS partition-leader route");
    Ok(())
}
