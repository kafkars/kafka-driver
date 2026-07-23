//! Bounded conversion of generated broker membership into one immutable generation.

use std::num::NonZeroU16;

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    EvidenceStamp, HostName, MetadataGeneration,
};
use kafka_wire::{MetadataResponse, metadata_response::MetadataResponseBroker};

use super::MetadataBuildError;

pub(super) fn broker_directory_from_response(
    response: &MetadataResponse,
    generation: MetadataGeneration,
    evidence: EvidenceStamp,
    limits: BrokerDirectoryLimits,
) -> Result<BrokerDirectory, MetadataBuildError> {
    let broker_limit = limits.max_brokers().get();
    if response.brokers.len() > broker_limit {
        return Err(MetadataBuildError::BrokerCapacity {
            observed: response.brokers.len(),
            limit: broker_limit,
        });
    }
    let entries = response
        .brokers
        .iter()
        .map(broker_entry)
        .collect::<Result<Vec<_>, _>>()?;
    BrokerDirectory::try_from_iter_with_evidence(generation, evidence, entries, limits)
        .map_err(MetadataBuildError::Directory)
}

fn broker_entry(
    broker: &MetadataResponseBroker,
) -> Result<BrokerDirectoryEntry, MetadataBuildError> {
    let broker_id = BrokerId::new(broker.node_id).map_err(MetadataBuildError::BrokerId)?;
    let host = HostName::new(broker.host.as_str())
        .map_err(|source| MetadataBuildError::BrokerHost { broker_id, source })?;
    let port = u16::try_from(broker.port)
        .ok()
        .and_then(NonZeroU16::new)
        .ok_or(MetadataBuildError::BrokerPort {
            broker_id,
            port: broker.port,
        })?;
    Ok(BrokerDirectoryEntry::new(
        broker_id,
        BrokerEndpoint::new(host, port),
    ))
}

pub(super) fn controller_id(raw: i32) -> Result<Option<BrokerId>, MetadataBuildError> {
    if raw == -1 {
        return Ok(None);
    }
    BrokerId::new(raw)
        .map(Some)
        .map_err(MetadataBuildError::ControllerId)
}
