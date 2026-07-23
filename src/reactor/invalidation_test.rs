//! Shared invalidation completion fanout scenarios.

use crate::{
    InvalidationDisposition, completion::completion_pair, reactor::InvalidationSubscribers,
};

#[test]
fn every_subscriber_receives_the_shared_terminal_disposition() {
    let (first, first_sender) = completion_pair();
    let (second, second_sender) = completion_pair();
    let mut subscribers = InvalidationSubscribers::new(first_sender);
    subscribers.subscribe(second_sender);

    subscribers.complete(InvalidationDisposition::Applied);

    assert_eq!(first.wait(), Ok(InvalidationDisposition::Applied));
    assert_eq!(second.wait(), Ok(InvalidationDisposition::Applied));
}
