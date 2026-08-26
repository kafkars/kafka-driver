//! Release builds retain Bornera as their sole connection and transport owner.

use std::{fs, path::Path};

use syn::Item;

use super::support::{read, workspace_root};

#[test]
fn legacy_transport_module_trees_are_absent() {
    let root = workspace_root();
    let source = read(&root.join("src/reactor/mod.rs"));
    let syntax = syn::parse_file(&source).unwrap_or_else(|error| panic!("parse reactor: {error}"));
    let legacy = [
        "broker_set",
        "plaintext",
        "poller",
        "resource",
        "tcp",
        "timer",
        "tls",
    ];
    let declared = syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Mod(module) if legacy.contains(&module.ident.to_string().as_str()) => {
                Some(module.ident.to_string())
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    assert!(
        declared.is_empty(),
        "legacy reactor modules remain declared: {declared:?}"
    );
    let retained = legacy
        .iter()
        .filter(|module| contains_rust_source(&root.join("src/reactor").join(module)))
        .collect::<Vec<_>>();
    assert!(
        retained.is_empty(),
        "legacy reactor module trees remain on disk: {retained:?}"
    );
}

fn contains_rust_source(directory: &Path) -> bool {
    let Ok(entries) = fs::read_dir(directory) else {
        return false;
    };
    entries.filter_map(Result::ok).any(|entry| {
        let path = entry.path();
        if path.is_dir() {
            contains_rust_source(&path)
        } else {
            path.extension().is_some_and(|extension| extension == "rs")
        }
    })
}

#[test]
fn reactor_backend_and_construction_have_no_legacy_path() {
    let root = workspace_root();
    let backend = read(&root.join("src/reactor/backend.rs"));
    let backend =
        syn::parse_file(&backend).unwrap_or_else(|error| panic!("parse backend: {error}"));
    let has_legacy_variant = backend.items.iter().any(|item| match item {
        Item::Enum(item) if item.ident == "ReactorBackend" => item
            .variants
            .iter()
            .any(|variant| variant.ident == "Legacy"),
        _ => false,
    });
    assert!(
        !has_legacy_variant,
        "ReactorBackend must have no legacy variant"
    );

    let construction = read(&root.join("src/reactor/host/construction.rs"));
    assert!(
        !construction.contains("new_legacy") && !construction.contains("LegacyBackend"),
        "reactor construction must have no legacy path"
    );
}
