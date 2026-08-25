//! Movement metadata validation stays independent of the public driver.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use kafka_wire::{
    MetadataResponse, metadata_response::MetadataResponseBroker,
    metadata_response::MetadataResponsePartition, metadata_response::MetadataResponseTopic,
};
use kafka_wire_core::StrBytes;

use super::metadata_observer::{validate, validate_fenced};

#[test]
fn exact_advertisement_leader_and_isr_are_ready() {
    let response = response(19092, 1, vec![1]);

    let observed = validate(&response, "moved", 1, endpoint(19092));

    assert_eq!(
        observed,
        Ok("brokers=[1=127.0.0.1:19092],partition=0,error=0,leader=1,isr=[1]".to_owned())
    );
}

#[test]
fn stale_advertisement_is_not_ready() {
    let response = response(9092, 1, vec![1]);

    let Err(error) = validate(&response, "moved", 1, endpoint(19092)) else {
        panic!("the old advertised port must not satisfy movement readiness");
    };

    assert!(
        error
            .to_string()
            .contains("expected broker advertisement is absent")
    );
    assert!(error.to_string().contains("1=127.0.0.1:9092"));
}

#[test]
fn leader_outside_the_isr_is_not_ready() {
    let response = response(19092, 1, Vec::new());

    let Err(error) = validate(&response, "moved", 1, endpoint(19092)) else {
        panic!("a leader outside the ISR must not satisfy movement readiness");
    };

    assert!(
        error
            .to_string()
            .contains("partition leader is not in sync")
    );
}

#[test]
fn absent_broker_and_leader_are_authoritatively_fenced() {
    let mut response = response(19092, -1, Vec::new());
    response.brokers.clear();

    let observed = validate_fenced(&response, "moved", 1);

    assert_eq!(
        observed,
        Ok("brokers=[],partition=0,error=0,leader=-1,isr=[]".to_owned())
    );
}

#[test]
fn registered_broker_is_not_authoritatively_fenced() {
    let response = response(19092, -1, Vec::new());

    let Err(error) = validate_fenced(&response, "moved", 1) else {
        panic!("a registered broker must not satisfy the fencing gate");
    };

    assert!(
        error
            .to_string()
            .contains("stopped broker remains registered")
    );
}

fn response(port: i32, leader_id: i32, isr_nodes: Vec<i32>) -> MetadataResponse {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = 1;
    broker.host = StrBytes::from("127.0.0.1");
    broker.port = port;
    let mut partition = MetadataResponsePartition::default();
    partition.partition_index = 0;
    partition.leader_id = leader_id;
    partition.replica_nodes = vec![1];
    partition.isr_nodes = isr_nodes;
    let mut topic = MetadataResponseTopic::default();
    topic.name = Some(StrBytes::from("moved"));
    topic.partitions.push(partition);
    let mut response = MetadataResponse::default();
    response.brokers.push(broker);
    response.topics.push(topic);
    response
}

const fn endpoint(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}
