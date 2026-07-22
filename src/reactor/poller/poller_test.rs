//! Real-selector scenarios for bounded polling and coalesced cross-thread wake.

use std::{num::NonZeroUsize, time::Duration};

use super::{PollEvent, selector::Poller};

#[test]
fn explicit_wake_releases_the_real_selector_without_sleeping() {
    let Ok(mut poller) = Poller::new(NonZeroUsize::MIN) else {
        panic!("host must provide a Mio selector");
    };
    let wake = poller.wake_handle();
    assert!(wake.wake().is_ok());
    let mut events = Vec::with_capacity(1);

    let result = poller.poll_into(Some(Duration::from_secs(1)), &mut events);

    let Ok(observed) = result else {
        panic!("wake poll must succeed");
    };
    assert_eq!(observed, 1);
    assert_eq!(events, vec![PollEvent::Wake]);
}

#[test]
fn zero_timeout_returns_an_empty_bounded_batch() {
    let Ok(mut poller) = Poller::new(NonZeroUsize::MIN) else {
        panic!("host must provide a Mio selector");
    };
    let mut events = Vec::with_capacity(1);

    let result = poller.poll_into(Some(Duration::ZERO), &mut events);

    let Ok(observed) = result else {
        panic!("nonblocking poll must succeed");
    };
    assert_eq!(observed, 0);
    assert!(events.is_empty());
}
