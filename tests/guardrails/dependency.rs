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
    let policy = &value["workspace"]["dependencies"]["rustls"];
    let transport_feature = value["features"]["tls-rustls"]
        .as_array()
        .unwrap_or_else(|| panic!("tls-rustls must be an explicit feature"));
    let policy_features = policy["features"]
        .as_array()
        .unwrap_or_else(|| panic!("workspace rustls policy must name exact features"));

    assert_eq!(rustls["workspace"].as_bool(), Some(true));
    assert_eq!(rustls["optional"].as_bool(), Some(true));
    assert_eq!(policy["default-features"].as_bool(), Some(false));
    assert_eq!(
        policy_features,
        &[
            toml::Value::String("ring".to_owned()),
            toml::Value::String("std".to_owned()),
        ]
    );
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
fn real_broker_probe_depends_only_on_public_driver_protocol_and_tls_surfaces() {
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
fn ci_delegates_validation_to_the_canonical_repository_gate() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/ci.yml"));

    assert_eq!(
        workflow.matches("run: scripts/check").count(),
        1,
        "CI must execute scripts/check exactly once instead of duplicating its policy"
    );
}

#[test]
fn ci_runs_the_same_gate_on_linux_macos_and_windows() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/ci.yml"));

    assert!(workflow.contains("os: [ubuntu-latest, macos-latest, windows-latest]"));
    assert!(workflow.contains("runs-on: ${{ matrix.os }}"));
}

#[test]
fn extensionless_gate_scripts_keep_unix_line_endings_on_every_runner() {
    let root = workspace_root();
    let attributes = read(&root.join(".gitattributes"));

    assert!(
        attributes
            .lines()
            .any(|line| line.trim() == "scripts/* text eol=lf"),
        "extensionless scripts must retain LF endings for Windows Git Bash"
    );
}

#[test]
fn release_qualification_is_scheduled_and_pins_the_protocol() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let workflow = read(&root.join(".github/workflows/qualification.yml"));
    let repository = format!(
        "repository: {}",
        guardrails.dependencies.kafka_wire_repository
    );
    let revision = format!("ref: {}", guardrails.dependencies.kafka_wire_revision);

    assert!(workflow.contains("schedule:"));
    assert!(workflow.contains("tags: [\"v*\"]"));
    assert!(workflow.contains(&repository));
    assert!(workflow.contains(&revision));
    assert!(!workflow.contains("KAFKA_PROTOCOL_SSH_KEY"));
    assert!(!workflow.contains("KAFKA_PROTOCOL_TOKEN"));
    assert!(workflow.contains("run: npm run qualify:real-kafka"));
}

#[test]
fn secure_cluster_qualification_binds_each_advertised_host_to_its_identity() {
    let root = workspace_root();
    let manifest = read(&root.join("package.json"));
    let compose = read(&root.join("smoke/kafka-secure-cluster.compose.yml"));
    let scenario = read(&root.join("smoke/real-kafka-secure-multi-broker.smoke.mjs"));
    let identities = read(&root.join("smoke/support/tls-identities.mjs"));

    assert!(manifest.contains(
        "\"smoke:real-kafka-secure-multi-broker\": \
         \"smoque run smoke/ --tag real-kafka-secure-multi-broker --ci\""
    ));
    assert!(compose.contains("image: rust:1.88.0-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0"));
    for (number, broker) in [(1, "kafka-1"), (2, "kafka-2"), (3, "kafka-3")] {
        assert!(compose.contains(&format!("SSL://{broker}:9092")));
        assert!(compose.contains(&format!("KAFKA_{number}_SSL_SECRETS")));
    }
    assert!(identities.contains("DNS.1 = ${brokerName}"));
    assert!(scenario.contains("\"kafka-1:9092,kafka-2:9092\""));
    assert!(scenario.contains("RECOVERED TLS broker failover 1"));
    assert!(scenario.contains("PASS TLS broker failover 2"));
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
