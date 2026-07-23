//! Boundary scenarios for persistent Kafka topic routing names.

use super::{TopicName, TopicNameError};

#[test]
fn topic_names_are_nonempty_and_bounded_by_utf8_bytes() {
    let maximum = "t".repeat(TopicName::MAX_BYTES);
    let overlong = "t".repeat(TopicName::MAX_BYTES + 1);

    assert_eq!(
        TopicName::new(maximum).map(|name| name.as_str().len()),
        Ok(TopicName::MAX_BYTES)
    );
    assert_eq!(TopicName::new(""), Err(TopicNameError::Empty));
    assert_eq!(
        TopicName::new(overlong),
        Err(TopicNameError::TooLong {
            bytes: TopicName::MAX_BYTES + 1,
            limit: TopicName::MAX_BYTES,
        })
    );
}

#[test]
fn topic_name_reports_its_owned_buffer_capacity() {
    let mut source = String::with_capacity(64);
    source.push_str("payments");
    let reserved = source.capacity();
    let topic =
        TopicName::new(source).unwrap_or_else(|error| panic!("valid topic rejected: {error}"));

    assert_eq!(topic.heap_bytes(), reserved);
}
