//! Public packages carry complete, exact, and archive-visible metadata.

use std::path::Path;

use toml::Value;

use super::support::{read, workspace_root};

const CANONICAL_REPOSITORY: &str = "https://github.com/kafkars/kafka-driver";

const PUBLIC_PACKAGES: [(&str, &str); 3] = [
    ("kafka-driver", "Cargo.toml"),
    ("kafka-driver-core", "crates/kafka-driver-core/Cargo.toml"),
    (
        "kafka-driver-transport",
        "crates/kafka-driver-transport/Cargo.toml",
    ),
];

#[test]
fn public_package_metadata_is_complete() {
    let root = workspace_root();
    let repository_license = read(&root.join("LICENSE"));
    let workspace = parse(&root.join("Cargo.toml"));
    assert_eq!(
        workspace["workspace"]["package"]["version"].as_str(),
        Some("0.1.0-rc.4")
    );
    assert_eq!(
        workspace["workspace"]["package"]["license"].as_str(),
        Some("Apache-2.0")
    );
    assert_eq!(
        workspace["workspace"]["package"]["repository"].as_str(),
        Some(CANONICAL_REPOSITORY)
    );

    for (name, manifest_path) in PUBLIC_PACKAGES {
        let manifest_path = root.join(manifest_path);
        let manifest = parse(&manifest_path);
        let package = &manifest["package"];
        assert_eq!(package["name"].as_str(), Some(name));
        assert_eq!(package["version"]["workspace"].as_bool(), Some(true));
        assert_eq!(package["license"]["workspace"].as_bool(), Some(true));
        assert_eq!(package["repository"]["workspace"].as_bool(), Some(true));
        assert_eq!(
            package["publish"]
                .as_array()
                .and_then(|values| { (values.len() == 1).then(|| values[0].as_str()).flatten() }),
            Some("crates-io")
        );
        assert!(
            package["description"]
                .as_str()
                .is_some_and(|value| !value.trim().is_empty()),
            "{name} description must be nonempty"
        );
        let Some(package_root) = manifest_path.parent() else {
            panic!("public package manifest must have a parent: {name}");
        };
        assert_eq!(
            read(&package_root.join("LICENSE")),
            repository_license,
            "{name} package LICENSE differs from the repository license"
        );
    }

    assert!(root.join("LICENSE").is_file(), "missing workspace license");
    assert!(root.join("README.md").is_file(), "missing public README");
    assert!(
        root.join("kafka-driver-logo.svg").is_file(),
        "missing public logo"
    );
    assert_eq!(workspace["package"]["readme"].as_str(), Some("README.md"));

    let package_qualification = read(&root.join("scripts/qualify-packages"));
    assert!(package_qualification.contains(CANONICAL_REPOSITORY));

    let repository_verification = read(&root.join("scripts/verify-release-repository"));
    assert!(repository_verification.contains(&format!("repository_url={CANONICAL_REPOSITORY}")));
    assert!(repository_verification.contains("--fail"));

    let release_workflow = read(&root.join(".github/workflows/qualification.yml"));
    assert!(release_workflow.contains("run: scripts/verify-release-repository"));
}

fn parse(path: &Path) -> Value {
    read(path)
        .parse::<Value>()
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}
