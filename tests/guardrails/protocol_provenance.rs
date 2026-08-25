//! Published protocol and connection dependencies remain exact crates.io artifacts.

use std::path::Path;

use super::support::{load_guardrails, read, workspace_root};

const CRATES_IO_SOURCE: &str = "registry+https://github.com/rust-lang/crates.io-index";

#[test]
fn protocol_lock_entries_match_the_audited_registry_artifacts() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let lock = parse(&root.join("Cargo.lock"));

    assert_locked(
        &lock,
        "kafka-wire",
        &guardrails.dependencies.kafka_wire_version,
        &guardrails.dependencies.kafka_wire_checksum,
    );
    assert_locked(
        &lock,
        "kafka-wire-core",
        &guardrails.dependencies.kafka_wire_core_version,
        &guardrails.dependencies.kafka_wire_core_checksum,
    );
    assert_locked(
        &lock,
        "bornera",
        &guardrails.dependencies.bornera_version,
        &guardrails.dependencies.bornera_checksum,
    );
    assert_locked(
        &lock,
        "bornera-core",
        &guardrails.dependencies.bornera_core_version,
        &guardrails.dependencies.bornera_core_checksum,
    );
    assert_locked(
        &lock,
        "bornera-rustls",
        &guardrails.dependencies.bornera_rustls_version,
        &guardrails.dependencies.bornera_rustls_checksum,
    );
}

#[test]
fn protocol_dependencies_have_no_source_override_escape_hatch() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let workspace = parse(&root.join("Cargo.toml"));
    let probe = parse(&root.join("crates/kafka-driver-probe/Cargo.toml"));

    for (package, expected) in [
        (
            "kafka-wire",
            guardrails.dependencies.kafka_wire_version.as_str(),
        ),
        (
            "kafka-wire-core",
            guardrails.dependencies.kafka_wire_core_version.as_str(),
        ),
        ("bornera", guardrails.dependencies.bornera_version.as_str()),
        (
            "bornera-core",
            guardrails.dependencies.bornera_core_version.as_str(),
        ),
        (
            "bornera-rustls",
            guardrails.dependencies.bornera_rustls_version.as_str(),
        ),
    ] {
        let dependency = &workspace["workspace"]["dependencies"][package];
        assert!(
            dependency.is_str(),
            "{package} must be an exact registry version string"
        );
        assert_eq!(
            dependency.as_str(),
            Some(expected),
            "{package} must be exact"
        );
    }
    assert_workspace_reference(&workspace["dependencies"]["kafka-wire"]);
    assert_workspace_reference(&workspace["dependencies"]["kafka-wire-core"]);
    assert_workspace_reference(&probe["dependencies"]["kafka-wire"]);
    assert_workspace_reference(&workspace["dependencies"]["bornera"]);
    assert_workspace_reference(&workspace["dependencies"]["bornera-core"]);
    assert_optional_workspace_reference(&workspace["dependencies"]["bornera-rustls"]);

    for manifest in [&workspace, &probe] {
        assert!(
            manifest.get("patch").is_none(),
            "registry patches are forbidden"
        );
        assert!(
            manifest.get("replace").is_none(),
            "dependency replacement is forbidden"
        );
        assert!(
            manifest.get("target").is_none(),
            "target-specific protocol redeclaration is forbidden"
        );
    }
}

fn assert_optional_workspace_reference(dependency: &toml::Value) {
    let table = dependency
        .as_table()
        .unwrap_or_else(|| panic!("optional dependency must use the workspace authority"));
    assert_eq!(
        table.len(),
        2,
        "optional dependency may only add its optional marker"
    );
    assert_eq!(
        table.get("workspace").and_then(toml::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        table.get("optional").and_then(toml::Value::as_bool),
        Some(true)
    );
}

fn assert_workspace_reference(dependency: &toml::Value) {
    let table = dependency
        .as_table()
        .unwrap_or_else(|| panic!("protocol consumer must use the workspace dependency"));
    assert_eq!(
        table.len(),
        1,
        "protocol dependency may not add source aliases"
    );
    assert_eq!(
        table.get("workspace").and_then(toml::Value::as_bool),
        Some(true)
    );
}

fn assert_locked(lock: &toml::Value, name: &str, version: &str, checksum: &str) {
    let version = version
        .strip_prefix('=')
        .unwrap_or_else(|| panic!("{name} guardrail version must be exact"));
    let packages = lock["package"]
        .as_array()
        .unwrap_or_else(|| panic!("Cargo.lock must contain package entries"));
    let matches = packages
        .iter()
        .filter(|package| package["name"].as_str() == Some(name))
        .collect::<Vec<_>>();

    assert_eq!(matches.len(), 1, "{name} must resolve exactly once");
    let package = matches[0];
    assert_eq!(package["version"].as_str(), Some(version));
    assert_eq!(package["source"].as_str(), Some(CRATES_IO_SOURCE));
    assert_eq!(package["checksum"].as_str(), Some(checksum));
}

fn parse(path: &Path) -> toml::Value {
    toml::from_str(&read(path)).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}
