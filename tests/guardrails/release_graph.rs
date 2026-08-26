//! Release builds retain Bornera as their sole connection and transport owner.

use syn::{Attribute, ImplItem, Item};

use super::support::{read, workspace_root};

fn is_test_only(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        attribute.path().is_ident("cfg")
            && attribute
                .meta
                .require_list()
                .is_ok_and(|list| list.tokens.to_string().contains("test"))
    })
}

#[test]
fn legacy_transport_modules_are_excluded_from_release_builds() {
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
    let exposed = syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Mod(module)
                if legacy.contains(&module.ident.to_string().as_str())
                    && !is_test_only(&module.attrs) =>
            {
                Some(module.ident.to_string())
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    assert!(
        exposed.is_empty(),
        "legacy reactor modules exposed in release graph: {exposed:?}"
    );
}

#[test]
fn legacy_backend_and_constructor_are_test_only() {
    let root = workspace_root();
    let backend = read(&root.join("src/reactor/backend.rs"));
    let backend =
        syn::parse_file(&backend).unwrap_or_else(|error| panic!("parse backend: {error}"));
    let legacy_variant = backend.items.iter().find_map(|item| match item {
        Item::Enum(item) if item.ident == "ReactorBackend" => item
            .variants
            .iter()
            .find(|variant| variant.ident == "Legacy"),
        _ => None,
    });
    assert!(
        legacy_variant.is_some_and(|variant| is_test_only(&variant.attrs)),
        "ReactorBackend::Legacy must remain test-only"
    );

    let construction = read(&root.join("src/reactor/host/construction.rs"));
    let construction = syn::parse_file(&construction)
        .unwrap_or_else(|error| panic!("parse construction: {error}"));
    let legacy_constructor = construction.items.iter().find_map(|item| match item {
        Item::Impl(item) => item.items.iter().find_map(|item| match item {
            ImplItem::Fn(function) if function.sig.ident == "new_legacy_test" => Some(function),
            _ => None,
        }),
        _ => None,
    });
    assert!(
        legacy_constructor.is_some_and(|function| is_test_only(&function.attrs)),
        "legacy reactor construction must remain test-only"
    );
}
