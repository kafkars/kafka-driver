//! Boundary scenarios for the immutable Kafka request-header identity.

use super::client_id::{ClientId, ClientIdError, MAX_CLIENT_ID_BYTES};

#[test]
fn client_id_accepts_the_wire_string_boundary_and_rejects_one_byte_more() {
    let maximum = "x".repeat(MAX_CLIENT_ID_BYTES);
    let retained = ClientId::try_new(maximum.clone())
        .unwrap_or_else(|error| panic!("maximum client identity must fit: {error:?}"));
    assert_eq!(retained.wire().as_str(), maximum);

    let oversized = "x".repeat(MAX_CLIENT_ID_BYTES + 1);
    assert_eq!(
        ClientId::try_new(oversized),
        Err(ClientIdError {
            actual: MAX_CLIENT_ID_BYTES + 1,
            limit: MAX_CLIENT_ID_BYTES,
        })
    );
}
