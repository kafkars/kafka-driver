//! Bounded topic-view terminal-capacity and deadline scenarios.

use std::num::NonZeroUsize;

use kafka_driver_core::{MetadataGeneration, MetadataMachine, Moment, TopicName};

use crate::{
    MetadataLimits, TopicView, TopicViewError,
    completion::completion_pair,
    reactor::metadata::{TopicViewWait, topic_waiting::TopicViewWaiters},
};

#[test]
fn default_pool_admits_one_maximum_topic_projection() {
    let limits = MetadataLimits::default();
    let result_capacity_bytes =
        TopicView::maximum_available_bytes(limits.partition_leaders().max_partitions());
    let (_receiver, completion) = completion_pair();
    let waiting = TopicViewWait::new(topic(), moment(10), result_capacity_bytes, completion);
    let retained = waiting.retained_bytes();
    assert!(retained <= limits.topic_view_bytes().get());
    let mut waiters = TopicViewWaiters::new(limits.topic_view_waiters(), limits.topic_view_bytes());

    assert!(waiters.admit(waiting));
}

#[test]
fn admission_reserves_the_conservative_terminal_projection_bytes() {
    let (receiver, completion) = completion_pair();
    let waiting = TopicViewWait::new(topic(), moment(10), 4_096, completion);
    let retained = waiting.retained_bytes();
    let mut waiters = TopicViewWaiters::new(nonzero(1), nonzero(retained - 1));

    assert!(!waiters.admit(waiting));
    assert!(matches!(
        receiver.try_result(),
        Some(Ok(Err(TopicViewError::CapacityReached {
            call_limit: 1,
            byte_limit,
        }))) if byte_limit == retained - 1
    ));
}

#[test]
fn exact_deadline_settles_without_waiting_for_metadata_progress() {
    let (receiver, completion) = completion_pair();
    let mut waiters = TopicViewWaiters::new(nonzero(1), nonzero(16_384));
    assert!(waiters.admit(TopicViewWait::new(topic(), moment(5), 0, completion)));
    let machine = MetadataMachine::new(MetadataGeneration::from_raw(1));

    let progress = waiters.scan(&machine, moment(5), nonzero(1));

    assert!(progress.made_progress());
    assert_eq!(
        receiver.try_result(),
        Some(Ok(Err(TopicViewError::DeadlineExceeded)))
    );
}

#[test]
fn exact_broker_terminal_wins_over_generic_unavailability() {
    let topic = topic();
    let (receiver, completion) = completion_pair();
    let mut waiters = TopicViewWaiters::new(nonzero(1), nonzero(16_384));
    assert!(waiters.admit(TopicViewWait::new(topic.clone(), moment(10), 0, completion,)));
    waiters.mark_terminal(&topic, TopicViewError::Broker { error_code: 3 });
    waiters.begin_scan();
    let machine = MetadataMachine::new(MetadataGeneration::from_raw(1));

    let progress = waiters.scan(&machine, moment(1), nonzero(1));

    assert!(progress.made_progress());
    assert_eq!(
        receiver.try_result(),
        Some(Ok(Err(TopicViewError::Broker { error_code: 3 })))
    );
}

fn topic() -> TopicName {
    TopicName::new("orders").unwrap_or_else(|error| panic!("valid topic: {error}"))
}

const fn moment(raw: u64) -> Moment {
    Moment::from_nanos(raw)
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
