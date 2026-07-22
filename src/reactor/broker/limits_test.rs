//! Scenarios for coherent single-broker capacity and fairness defaults.

use super::limits::BrokerLimits;

#[test]
fn given_default_limits_then_every_pending_call_has_response_and_timer_capacity() {
    // Given
    let limits = BrokerLimits::default();

    // Then
    assert_eq!(
        limits.response_capacity(),
        limits.connection().max_in_flight()
    );
    assert_eq!(limits.timer_capacity(), limits.connection().max_in_flight());
    assert_eq!(limits.resource_capacity().get(), 1);
    assert_eq!(limits.timer_budget().get(), 256);
    assert_eq!(limits.negotiation().max_advertised_apis(), 256);
    assert_eq!(limits.negotiation().max_negotiated_apis().get(), 128);
    assert_eq!(
        limits.negotiation_timeout(),
        std::time::Duration::from_secs(10)
    );
    assert_eq!(
        limits.outbound_frame().max_frame_bytes(),
        limits.plaintext().outbound_frame_bytes()
    );
    let _ = limits.plaintext();
    let _ = limits.read_budget();
    let _ = limits.write_budget();
}
