//! Conservative delivery translation proofs.

use kafka_driver_core::Delivery;

use super::driver_delivery;

#[test]
fn bornera_delivery_certainty_never_strengthens_at_the_driver_boundary() {
    assert_eq!(
        driver_delivery(bornera_core::Delivery::NotSent),
        Delivery::NotSent
    );
    assert_eq!(
        driver_delivery(bornera_core::Delivery::PossiblySent),
        Delivery::PossiblySent
    );
}
