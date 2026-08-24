//! Real-selector scenarios for bounded polling, coalesced wakes, and pulses.

use std::{num::NonZeroUsize, time::Duration};

use crate::reactor::WakeHandle;

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

#[test]
fn explicit_wake_handle_can_notify_again_after_the_first_event_is_consumed() {
    let Ok(mut poller) = Poller::new(NonZeroUsize::MIN) else {
        panic!("host must provide a Mio selector");
    };
    let wake = WakeHandle::new(poller.pulse_handle());
    let mut events = Vec::with_capacity(1);

    for _ in 0..2 {
        assert!(wake.wake().is_ok());
        let observed = poller
            .poll_into(Some(Duration::from_secs(1)), &mut events)
            .unwrap_or_else(|error| panic!("wake poll must succeed: {error}"));
        assert_eq!(observed, 1);
        assert_eq!(events, vec![PollEvent::Wake]);
        events.clear();
    }
}
