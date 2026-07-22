//! The complete dependency graph stays runtime-neutral and protocol-backed.

use std::collections::BTreeSet;

use super::support::{load_guardrails, read, workspace_root};

fn lockfile_packages(lockfile: &str) -> BTreeSet<&str> {
    lockfile
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("name = \"")
                .and_then(|name| name.strip_suffix('"'))
        })
        .collect()
}

#[test]
fn dependency_graph_contains_no_async_runtime() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let lockfile = read(&root.join("Cargo.lock"));
    let packages = lockfile_packages(&lockfile);
    let violations = guardrails
        .dependencies
        .banned
        .iter()
        .filter(|name| packages.contains(name.as_str()))
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "forbidden packages in Cargo.lock: {violations:?}"
    );
}

#[test]
fn kafka_wire_remains_the_local_protocol_authority() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let manifest = read(&root.join("Cargo.toml"));
    let value = manifest
        .parse::<toml::Value>()
        .unwrap_or_else(|error| panic!("parse Cargo.toml: {error}"));
    let path = value["dependencies"]["kafka-wire"]["path"]
        .as_str()
        .unwrap_or_else(|| panic!("kafka-wire must be a path dependency"));
    let core_path = value["workspace"]["dependencies"]["kafka-wire-core"]["path"]
        .as_str()
        .unwrap_or_else(|| panic!("kafka-wire-core must be a workspace path dependency"));

    assert_eq!(path, guardrails.dependencies.kafka_wire_path);
    assert_eq!(core_path, guardrails.dependencies.kafka_wire_core_path);
}

#[test]
fn driver_dependencies_are_explicitly_allowlisted() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let dependencies = manifest_dependencies(&root.join("Cargo.toml"));
    let allowed = guardrails
        .dependencies
        .driver_allowed
        .into_iter()
        .collect::<BTreeSet<_>>();

    assert_eq!(dependencies, allowed);
}

#[test]
fn rustls_is_a_runtime_neutral_optional_transport_feature() {
    let root = workspace_root();
    let manifest = read(&root.join("Cargo.toml"));
    let value = manifest
        .parse::<toml::Value>()
        .unwrap_or_else(|error| panic!("parse Cargo.toml: {error}"));
    let rustls = &value["dependencies"]["rustls"];
    let transport_feature = value["features"]["tls-rustls"]
        .as_array()
        .unwrap_or_else(|| panic!("tls-rustls must be an explicit feature"));

    assert_eq!(rustls["optional"].as_bool(), Some(true));
    assert_eq!(rustls["default-features"].as_bool(), Some(false));
    assert_eq!(
        transport_feature,
        &[toml::Value::String("dep:rustls".to_owned())]
    );
}

#[test]
fn deterministic_core_depends_only_on_protocol_authority() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let dependencies = manifest_dependencies(&root.join("crates/kafka-driver-core/Cargo.toml"));
    let allowed = guardrails
        .dependencies
        .core_allowed
        .into_iter()
        .collect::<BTreeSet<_>>();
    let violations = dependencies.difference(&allowed).collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "the deterministic core acquired forbidden dependencies: {violations:?}"
    );
}

#[test]
fn transport_depends_only_on_deterministic_driver_and_wire_primitives() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let dependencies =
        manifest_dependencies(&root.join("crates/kafka-driver-transport/Cargo.toml"));
    let allowed = guardrails
        .dependencies
        .transport_allowed
        .into_iter()
        .collect::<BTreeSet<_>>();

    assert_eq!(dependencies, allowed);
}

#[test]
fn simulator_depends_only_on_the_deterministic_core() {
    let root = workspace_root();
    let dependencies = manifest_dependencies(&root.join("crates/kafka-driver-sim/Cargo.toml"));

    assert_eq!(
        dependencies,
        BTreeSet::from(["kafka-driver-core".to_owned()])
    );
}

#[test]
fn real_broker_probe_depends_only_on_public_driver_and_protocol_surfaces() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let dependencies = manifest_dependencies(&root.join("crates/kafka-driver-probe/Cargo.toml"));
    let allowed = guardrails
        .dependencies
        .probe_allowed
        .into_iter()
        .collect::<BTreeSet<_>>();

    assert_eq!(dependencies, allowed);
}

#[test]
fn ci_pins_the_verified_public_kafka_wire_source() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let workflow = read(&root.join(".github/workflows/ci.yml"));
    let repository = format!(
        "repository: {}",
        guardrails.dependencies.kafka_wire_repository
    );
    let revision = format!("ref: {}", guardrails.dependencies.kafka_wire_revision);

    assert!(workflow.contains(&repository));
    assert!(workflow.contains(&revision));
    assert!(!workflow.contains("KAFKA_PROTOCOL_SSH_KEY"));
    assert!(!workflow.contains("KAFKA_PROTOCOL_TOKEN"));
}

#[test]
fn package_extraction_matches_exact_names_only() {
    let packages = lockfile_packages(
        "[[package]]\nname = \"tokio-util-extra\"\n[[package]]\nname = \"bytes\"\n",
    );

    assert!(!packages.contains("tokio"));
    assert!(packages.contains("bytes"));
}

fn manifest_dependencies(path: &std::path::Path) -> BTreeSet<String> {
    let manifest = read(path);
    let value = manifest
        .parse::<toml::Value>()
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));

    value["dependencies"]
        .as_table()
        .map_or_else(BTreeSet::new, |dependencies| {
            dependencies.keys().cloned().collect()
        })
}
