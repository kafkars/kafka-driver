//! Request-shape scenario for bounded initial cluster-membership discovery.

use super::owner::broker_metadata_request;

#[test]
fn initial_refresh_does_not_expand_an_unbounded_all_topics_response() {
    let request = broker_metadata_request();

    assert!(request.topics.is_some_and(|topics| topics.is_empty()));
}
