//! Scenarios proving delivery certainty is monotonic.

use super::Delivery;

#[test]
fn possibly_sent_never_returns_to_not_sent() {
    let delivery = Delivery::PossiblySent;

    let combined = delivery.combine(Delivery::NotSent);

    assert_eq!(combined, Delivery::PossiblySent);
}

#[test]
fn two_not_sent_observations_remain_not_sent() {
    let delivery = Delivery::NotSent;

    let combined = delivery.combine(Delivery::NotSent);

    assert_eq!(combined, Delivery::NotSent);
}
