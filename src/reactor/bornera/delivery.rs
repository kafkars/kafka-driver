//! Conservative delivery translation at the private ownership boundary.

use kafka_driver_core::Delivery;

pub(in crate::reactor) const fn driver_delivery(delivery: bornera_core::Delivery) -> Delivery {
    match delivery {
        bornera_core::Delivery::NotSent => Delivery::NotSent,
        bornera_core::Delivery::PossiblySent => Delivery::PossiblySent,
    }
}
