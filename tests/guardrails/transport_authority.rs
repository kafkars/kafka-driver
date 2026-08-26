//! Syntax-level ownership guard for the sole selector and transport adapter.
//!
//! This catches accidental source regressions, not semantic name resolution or macro expansion.
//! Imports of guarded authority names therefore may not be renamed.

use std::collections::{BTreeMap, BTreeSet};

use syn::{ExprMethodCall, ExprPath, File, ItemImpl, ItemUse, Path, Type, UseTree, visit::Visit};

use super::support::{display_path, is_test, read, rust_files, workspace_root};

const SET_OWNER: &str = "src/reactor/direct_plaintext/set_owner.rs";
const RUSTLS_ADAPTER: &str = "src/reactor/direct_plaintext/rustls_transport.rs";
const ASSOCIATED_CALLS: [&str; 13] = [
    "ConnectionSet::new",
    "ConnectionSet::turn_component",
    "ConnectionSet::poll_io",
    "ConnectionSet::wake_handle",
    "ConnectionSet::pulse_handle",
    "DirectSet::new",
    "DirectSet::turn_component",
    "DirectSet::poll_io",
    "DirectSet::wake_handle",
    "DirectSet::pulse_handle",
    "Source::register",
    "Source::reregister",
    "Source::deregister",
];
const GUARDED_RENAMES: [&str; 5] = [
    "ConnectionSet",
    "DirectSet",
    "RegisteredTransport",
    "SlotTransport",
    "Source",
];
const SELECTOR_METHODS: [&str; 4] = ["turn_component", "poll_io", "wake_handle", "pulse_handle"];

#[derive(Debug, Default, Eq, PartialEq)]
struct AuthorityInventory {
    connection_set_files: BTreeSet<String>,
    associated_calls: BTreeMap<String, usize>,
    renamed_authorities: BTreeSet<String>,
    selector_methods: BTreeMap<String, usize>,
    transport_impls: BTreeSet<String>,
}

#[test]
fn selector_and_transport_authority_matches_the_reviewed_boundary() {
    let actual = repository_inventory();
    assert_eq!(
        actual.connection_set_files,
        BTreeSet::from([SET_OWNER.into()])
    );
    assert_eq!(actual.associated_calls, expected_associated_calls());
    assert_eq!(actual.renamed_authorities, BTreeSet::new());
    assert_eq!(actual.selector_methods, expected_selector_methods());
    assert_eq!(actual.transport_impls, expected_transport_impls());
}

#[test]
fn inventory_detects_alias_ufcs_and_guarded_renames() {
    let source = r"
        use bornera::{
            ConnectionSet as Set,
            RegisteredTransport as Rt,
            SlotTransport as St,
        };
        use crate::reactor::direct_plaintext::set_owner::DirectSet;
        use crate::reactor::direct_plaintext::set_owner::DirectSet as SetAlias;
        use mio::event::Source as IoSource;

        fn rogue(mut set: DirectSet<T>) {
            let _ = ConnectionSet::new(config, limits);
            let _ = DirectSet::<T>::new(config, limits);
            let poll = DirectSet::poll_io;
            let _ = poll(&mut set, maximum);
            let _ = DirectSet::turn_component(&mut set, now);
            let _ = DirectSet::wake_handle(&set);
            let _ = DirectSet::pulse_handle(&set);
            let _ = set.poll_io(span);
            let _ = Set::<Decoder, Classifier, T>::new(config, limits);
        }
        struct Rogue;
        impl RegisteredTransport for Rogue {}
        impl Rt for Rogue {}
        impl St for Rogue {}
        impl IoSource for DirectRustlsTransport {}
    ";
    let actual = source_inventory("src/reactor/rogue.rs", source);
    assert_eq!(
        actual.connection_set_files,
        BTreeSet::from(["src/reactor/rogue.rs".into()])
    );
    assert_eq!(
        actual.associated_calls,
        counts(&[
            ("src/reactor/rogue.rs:ConnectionSet::new", 1),
            ("src/reactor/rogue.rs:DirectSet::new", 1),
            ("src/reactor/rogue.rs:DirectSet::poll_io", 1),
            ("src/reactor/rogue.rs:DirectSet::turn_component", 1),
            ("src/reactor/rogue.rs:DirectSet::wake_handle", 1),
            ("src/reactor/rogue.rs:DirectSet::pulse_handle", 1),
        ])
    );
    assert_eq!(
        actual.renamed_authorities,
        [
            "ConnectionSet as Set",
            "DirectSet as SetAlias",
            "RegisteredTransport as Rt",
            "SlotTransport as St",
            "Source as IoSource",
        ]
        .map(|rename| format!("src/reactor/rogue.rs:{rename}"))
        .into_iter()
        .collect()
    );
    assert_eq!(
        actual.selector_methods,
        counts(&[("src/reactor/rogue.rs:poll_io", 1)])
    );
    assert_eq!(
        actual.transport_impls,
        BTreeSet::from(["src/reactor/rogue.rs:Rogue:RegisteredTransport".into()])
    );
}

fn repository_inventory() -> AuthorityInventory {
    let root = workspace_root();
    let mut inventory = AuthorityInventory::default();
    for path in rust_files(&root) {
        if is_test(&root, &path) {
            continue;
        }
        let relative = display_path(&root, &path);
        let source = read(&path);
        let syntax =
            syn::parse_file(&source).unwrap_or_else(|error| panic!("parse {relative}: {error}"));
        inspect(&syntax, &relative, &mut inventory);
    }
    inventory
}

fn source_inventory(path: &str, source: &str) -> AuthorityInventory {
    let syntax = syn::parse_file(source)
        .unwrap_or_else(|error| panic!("parse adversarial authority source: {error}"));
    let mut inventory = AuthorityInventory::default();
    inspect(&syntax, path, &mut inventory);
    inventory
}

fn inspect(syntax: &File, path: &str, inventory: &mut AuthorityInventory) {
    AuthorityVisitor { path, inventory }.visit_file(syntax);
}

struct AuthorityVisitor<'a> {
    path: &'a str,
    inventory: &'a mut AuthorityInventory,
}

impl<'ast> Visit<'ast> for AuthorityVisitor<'_> {
    fn visit_path(&mut self, path: &'ast Path) {
        if path
            .segments
            .iter()
            .any(|segment| segment.ident == "ConnectionSet")
        {
            self.inventory.connection_set_files.insert(self.path.into());
        }
        syn::visit::visit_path(self, path);
    }

    fn visit_expr_path(&mut self, path: &'ast ExprPath) {
        if let Some(authority) = associated_authority(&path.path) {
            increment(
                &mut self.inventory.associated_calls,
                format!("{}:{authority}", self.path),
            );
        }
        syn::visit::visit_expr_path(self, path);
    }

    fn visit_expr_method_call(&mut self, call: &'ast ExprMethodCall) {
        let method = call.method.to_string();
        if SELECTOR_METHODS.contains(&method.as_str()) {
            increment(
                &mut self.inventory.selector_methods,
                format!("{}:{method}", self.path),
            );
        }
        syn::visit::visit_expr_method_call(self, call);
    }

    fn visit_item_impl(&mut self, item: &'ast ItemImpl) {
        if let Some((_, trait_path, _)) = &item.trait_
            && let Some(trait_name) = trait_path.segments.last()
            && let Some(type_name) = type_name(&item.self_ty)
            && (matches!(
                trait_name.ident.to_string().as_str(),
                "RegisteredTransport" | "SlotTransport"
            ) || (trait_name.ident == "Source" && type_name == "DirectRustlsTransport"))
        {
            self.inventory
                .transport_impls
                .insert(format!("{}:{type_name}:{}", self.path, trait_name.ident));
        }
        syn::visit::visit_item_impl(self, item);
    }

    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        record_guarded_renames(
            &item.tree,
            self.path,
            &mut self.inventory.renamed_authorities,
        );
        syn::visit::visit_item_use(self, item);
    }
}

fn record_guarded_renames(tree: &UseTree, path: &str, renames: &mut BTreeSet<String>) {
    match tree {
        UseTree::Path(tree) => record_guarded_renames(&tree.tree, path, renames),
        UseTree::Group(group) => {
            for tree in &group.items {
                record_guarded_renames(tree, path, renames);
            }
        }
        UseTree::Rename(rename) => {
            let authority = rename.ident.to_string();
            if GUARDED_RENAMES.contains(&authority.as_str()) {
                renames.insert(format!("{path}:{authority} as {}", rename.rename));
            }
        }
        UseTree::Name(_) | UseTree::Glob(_) => {}
    }
}

fn associated_authority(path: &Path) -> Option<String> {
    let segments = path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>();
    let [.., owner, method] = segments.as_slice() else {
        return None;
    };
    let authority = format!("{owner}::{method}");
    ASSOCIATED_CALLS
        .contains(&authority.as_str())
        .then_some(authority)
}

fn type_name(ty: &Type) -> Option<String> {
    let Type::Path(path) = ty else {
        return None;
    };
    path.path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
}

fn increment(counts: &mut BTreeMap<String, usize>, key: String) {
    *counts.entry(key).or_default() += 1;
}

fn counts(entries: &[(&str, usize)]) -> BTreeMap<String, usize> {
    entries
        .iter()
        .map(|&(key, value)| (key.into(), value))
        .collect()
}

fn expected_associated_calls() -> BTreeMap<String, usize> {
    counts(&[
        (&format!("{SET_OWNER}:ConnectionSet::new"), 1),
        (&format!("{SET_OWNER}:ConnectionSet::turn_component"), 1),
        (&format!("{SET_OWNER}:ConnectionSet::poll_io"), 1),
        (&format!("{SET_OWNER}:ConnectionSet::wake_handle"), 1),
        (&format!("{SET_OWNER}:ConnectionSet::pulse_handle"), 1),
        (&format!("{RUSTLS_ADAPTER}:Source::register"), 1),
        (&format!("{RUSTLS_ADAPTER}:Source::reregister"), 1),
        (&format!("{RUSTLS_ADAPTER}:Source::deregister"), 1),
    ])
}

fn expected_selector_methods() -> BTreeMap<String, usize> {
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

fn expected_transport_impls() -> BTreeSet<String> {
    [
        format!("{RUSTLS_ADAPTER}:DirectRustlsTransport:RegisteredTransport"),
        format!("{RUSTLS_ADAPTER}:DirectRustlsTransport:SlotTransport"),
        format!("{RUSTLS_ADAPTER}:DirectRustlsTransport:Source"),
    ]
    .into_iter()
    .collect()
}
