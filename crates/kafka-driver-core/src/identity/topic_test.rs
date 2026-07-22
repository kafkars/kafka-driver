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
