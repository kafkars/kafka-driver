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
    let _ = limits.plaintext();
    let _ = limits.read_budget();
    let _ = limits.write_budget();
}
