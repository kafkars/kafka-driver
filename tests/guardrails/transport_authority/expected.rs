//! Reviewed selector calls and transport implementations expected by the authority guard.

use std::collections::{BTreeMap, BTreeSet};

const SET_OWNER: &str = "src/reactor/direct_plaintext/set_owner.rs";
const PLAINTEXT_ADAPTER: &str = "src/reactor/direct_plaintext/plaintext_transport.rs";
const RUSTLS_ADAPTER: &str = "src/reactor/direct_plaintext/rustls_transport.rs";

pub(super) fn expected_associated_calls() -> BTreeMap<String, usize> {
    counts(&[
        (&format!("{SET_OWNER}:ConnectionSet::new"), 1),
        (&format!("{SET_OWNER}:ConnectionSet::turn_component"), 1),
        (&format!("{SET_OWNER}:ConnectionSet::poll_io"), 1),
        (&format!("{SET_OWNER}:ConnectionSet::wake_handle"), 1),
        (&format!("{SET_OWNER}:ConnectionSet::pulse_handle"), 1),
        (&format!("{PLAINTEXT_ADAPTER}:Source::register"), 1),
        (&format!("{PLAINTEXT_ADAPTER}:Source::reregister"), 1),
        (&format!("{PLAINTEXT_ADAPTER}:Source::deregister"), 1),
        (&format!("{RUSTLS_ADAPTER}:Source::register"), 1),
        (&format!("{RUSTLS_ADAPTER}:Source::reregister"), 1),
        (&format!("{RUSTLS_ADAPTER}:Source::deregister"), 1),
    ])
}

pub(super) fn expected_selector_methods() -> BTreeMap<String, usize> {
    counts(&[
        ("src/reactor/backend.rs:wake_handle", 2),
        ("src/reactor/backend.rs:pulse_handle", 2),
        ("src/reactor/host.rs:wake_handle", 1),
        ("src/reactor/host/construction.rs:wake_handle", 1),
        ("src/reactor/host/construction.rs:pulse_handle", 2),
        ("src/reactor/direct_plaintext/backend.rs:wake_handle", 3),
        ("src/reactor/direct_plaintext/backend.rs:pulse_handle", 3),
        ("src/reactor/direct_plaintext/runtime.rs:wake_handle", 1),
        ("src/reactor/direct_plaintext/runtime.rs:pulse_handle", 1),
        (
            "src/reactor/direct_plaintext/cluster_runtime/backend.rs:wake_handle",
            2,
        ),
        (
            "src/reactor/direct_plaintext/cluster_runtime/backend.rs:pulse_handle",
            2,
        ),
    ])
}

pub(super) fn expected_transport_impls() -> BTreeSet<String> {
    [
        format!("{PLAINTEXT_ADAPTER}:DirectPlaintextTransport:RegisteredTransport"),
        format!("{PLAINTEXT_ADAPTER}:DirectPlaintextTransport:SlotTransport"),
        format!("{PLAINTEXT_ADAPTER}:DirectPlaintextTransport:Source"),
        format!("{RUSTLS_ADAPTER}:DirectRustlsTransport:RegisteredTransport"),
        format!("{RUSTLS_ADAPTER}:DirectRustlsTransport:SlotTransport"),
        format!("{RUSTLS_ADAPTER}:DirectRustlsTransport:Source"),
    ]
    .into_iter()
    .collect()
}

fn counts(entries: &[(&str, usize)]) -> BTreeMap<String, usize> {
    entries
        .iter()
        .map(|&(key, value)| (key.into(), value))
        .collect()
}
