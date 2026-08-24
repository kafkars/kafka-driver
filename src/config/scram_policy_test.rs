//! Given/When/Then checks for every bound in the Kafka SCRAM policy.

use super::scram_policy::{
    MAX_ATTRIBUTES, MAX_AUTHORIZATION_IDENTITY_BYTES, MAX_CHANNEL_BINDING_BYTES,
    MAX_EXTENSION_BYTES, MAX_IDENTITY_BYTES, MAX_ITERATIONS, MAX_MESSAGE_BYTES, MAX_NONCE_BYTES,
    MAX_SALT_BYTES, MIN_ITERATIONS, MIN_SALT_BYTES, kafka_scram_policy,
};

#[test]
fn kafka_scram_policy_names_every_resource_and_derivation_bound() {
    let policy = kafka_scram_policy().unwrap_or_else(|error| panic!("valid policy: {error:?}"));
    let resources = policy.resources();

    assert_eq!(resources.max_message_bytes(), MAX_MESSAGE_BYTES);
    assert_eq!(resources.max_attributes(), MAX_ATTRIBUTES);
    assert_eq!(resources.max_identity_bytes(), MAX_IDENTITY_BYTES);
    assert_eq!(
        resources.max_authorization_identity_bytes(),
        MAX_AUTHORIZATION_IDENTITY_BYTES
    );
    assert_eq!(resources.max_nonce_bytes(), MAX_NONCE_BYTES);
    assert_eq!(resources.max_salt_bytes(), MAX_SALT_BYTES);
    assert_eq!(
        resources.max_channel_binding_bytes(),
        MAX_CHANNEL_BINDING_BYTES
    );
    assert_eq!(resources.max_extension_bytes(), MAX_EXTENSION_BYTES);
    assert_eq!(policy.iterations().minimum().get(), MIN_ITERATIONS);
    assert_eq!(policy.iterations().maximum().get(), MAX_ITERATIONS);
    assert_eq!(policy.salt().minimum_bytes().get(), MIN_SALT_BYTES);
    assert_eq!(policy.salt().maximum_bytes().get(), MAX_SALT_BYTES);
}
