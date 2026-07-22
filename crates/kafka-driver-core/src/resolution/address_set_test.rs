//! Scenarios for bounded resolver results and stable address preference.

use std::num::{NonZeroU16, NonZeroUsize};

use crate::{IpAddress, ResolvedAddress};

use super::{ResolutionLimits, ResolvedAddressSet, ResolvedAddressSetError};

#[test]
fn resolver_order_is_stable_and_duplicate_addresses_are_coalesced() {
    let one = address([127, 0, 0, 1]);
    let two = address([127, 0, 0, 2]);

    let set = ResolvedAddressSet::try_from_iter([two, one, two], ResolutionLimits::default())
        .unwrap_or_else(|error| panic!("valid resolver result: {error}"));

    assert_eq!(set.iter().copied().collect::<Vec<_>>(), [two, one]);
    assert_eq!(set.len(), 2);
    assert!(!set.is_empty());
}

#[test]
fn resolver_admission_bounds_inspected_entries_before_deduplication() {
    let address = address([127, 0, 0, 1]);
    let limits = ResolutionLimits::new(nonzero_size(2));

    assert_eq!(
        ResolvedAddressSet::try_from_iter([address, address, address], limits),
        Err(ResolvedAddressSetError::Capacity { limit: 2 })
    );
}

#[test]
fn empty_successful_resolution_is_rejected() {
    assert_eq!(
        ResolvedAddressSet::try_from_iter([], ResolutionLimits::default()),
        Err(ResolvedAddressSetError::Empty)
    );
}

fn address(octets: [u8; 4]) -> ResolvedAddress {
    ResolvedAddress::new(IpAddress::V4(octets), port())
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}

fn nonzero_size(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
