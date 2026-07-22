//! Scenarios for observing exact active and queued Metadata query ownership.

use crate::{
    MetadataGeneration, MetadataInput, MetadataMachine, MetadataQuery, OperationId, TopicName,
};

#[test]
fn exact_query_observation_includes_active_and_queued_fetches() {
    let mut machine = MetadataMachine::new(MetadataGeneration::from_raw(1));
    let cluster = MetadataQuery::Cluster;
    let topic_query = MetadataQuery::Topic(topic("orders"));

    let _ = machine.apply(resolve(cluster.clone(), 1));
    let _ = machine.apply(resolve(topic_query.clone(), 2));

    assert!(machine.query_pending(&cluster));
    assert!(machine.query_pending(&topic_query));
    assert!(!machine.query_pending(&MetadataQuery::Topic(topic("payments"))));
}

fn resolve(query: MetadataQuery, raw_operation: u64) -> MetadataInput {
    MetadataInput::Resolve {
        query,
        operation_id: OperationId::from_raw(raw_operation),
    }
}

fn topic(value: &str) -> TopicName {
    TopicName::new(value).unwrap_or_else(|error| panic!("valid topic rejected: {error}"))
}
