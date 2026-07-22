//! Deterministic core source cannot acquire real-world capabilities.

use super::support::{display_path, load_guardrails, read, rust_files, workspace_root};

fn source_violations(relative: &str, source: &str, forbidden: &[String]) -> Vec<String> {
    forbidden
        .iter()
        .filter(|token| source.contains(token.as_str()))
        .map(|token| format!("{relative} names forbidden capability `{token}`"))
        .collect()
}

#[test]
fn deterministic_roots_name_no_external_capabilities() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let mut violations = Vec::new();

    for rule in guardrails.capabilities {
        let capability_root = root.join(&rule.root);
        for path in rust_files(&root)
            .into_iter()
            .filter(|path| path.starts_with(&capability_root))
        {
            violations.extend(source_violations(
                &display_path(&root, &path),
                &read(&path),
                &rule.forbidden,
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "capability boundary violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn a_socket_import_is_rejected_by_the_detector() {
    let forbidden = vec!["std::net".to_owned()];
    let violations = source_violations(
        "crates/kafka-driver-core/src/connection.rs",
        "use std::net::TcpStream;",
        &forbidden,
    );

    assert_eq!(
        violations,
        vec!["crates/kafka-driver-core/src/connection.rs names forbidden capability `std::net`"]
    );
}

#[test]
fn configured_capability_roots_exist() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);

    for rule in guardrails.capabilities {
        assert!(
            root.join(rule.root).is_dir(),
            "configured capability root must exist"
        );
    }
}
