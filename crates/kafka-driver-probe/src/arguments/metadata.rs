//! Admission for driver-independent Kafka metadata observation commands.

use super::{ArgumentError, Arguments};

pub(super) fn parse(
    bootstrap: &str,
    topic: &str,
    broker_id: &str,
    advertised: Option<&str>,
) -> Result<Arguments, ArgumentError> {
    let broker_id = broker_id
        .parse::<i32>()
        .ok()
        .filter(|broker_id| *broker_id >= 0)
        .ok_or(ArgumentError::BrokerId)?;
    Ok(Arguments::Metadata {
        bootstrap: bootstrap.to_owned(),
        topic: topic.to_owned(),
        broker_id,
        advertised: advertised.map(str::to_owned),
    })
}
