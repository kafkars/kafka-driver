//! Metadata observer command arguments retain their exact authority boundary.

use super::arguments::{ArgumentError, Arguments};

#[test]
fn metadata_retains_the_authority_and_expected_advertisement() {
    let parsed = Arguments::parse(strings([
        "metadata",
        "127.0.0.1:9094",
        "moved-partition",
        "1",
        "127.0.0.1:19092",
    ]));

    assert_eq!(
        parsed,
        Ok(Arguments::Metadata {
            bootstrap: "127.0.0.1:9094".to_owned(),
            topic: "moved-partition".to_owned(),
            broker_id: 1,
            advertised: Some("127.0.0.1:19092".to_owned()),
        })
    );
}

#[test]
fn metadata_fencing_retains_the_authority_topic_and_broker() {
    let parsed = Arguments::parse(strings([
        "metadata-fenced",
        "127.0.0.1:9094",
        "moved-partition",
        "1",
    ]));

    assert_eq!(
        parsed,
        Ok(Arguments::Metadata {
            bootstrap: "127.0.0.1:9094".to_owned(),
            topic: "moved-partition".to_owned(),
            broker_id: 1,
            advertised: None,
        })
    );
}

#[test]
fn metadata_rejects_an_invalid_broker_id() {
    for broker_id in ["broker-one", "-1"] {
        assert_eq!(
            Arguments::parse(strings([
                "metadata",
                "127.0.0.1:9094",
                "moved-partition",
                broker_id,
                "127.0.0.1:19092",
            ])),
            Err(ArgumentError::BrokerId)
        );
    }
}

#[test]
fn partial_metadata_commands_are_rejected() {
    assert_eq!(
        Arguments::parse(strings(["metadata", "one:1", "topic", "1"])),
        Err(ArgumentError::Shape)
    );
    assert_eq!(
        Arguments::parse(strings(["metadata-fenced", "one:1", "topic"])),
        Err(ArgumentError::Shape)
    );
}

fn strings<const N: usize>(values: [&str; N]) -> impl Iterator<Item = String> {
    values.into_iter().map(str::to_owned)
}
