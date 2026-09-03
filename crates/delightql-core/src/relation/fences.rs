// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Structural assertions over the authority's own source.
//!
//! The authority is crate-private, so an external compile-fail harness
//! cannot name its surface without widening that surface merely to test it.
//! These walk the source instead and assert the shape.
//!
//! A count is a change DETECTOR; the fence is always VISIBILITY. What is
//! proved here is that the visibility is what it claims to be — that no
//! field became public, no raw pairing constructor appeared, no setter was
//! added, and no wildcard arm crept into a law that must stay total.
//!
//! The walk FAILS CLOSED. Every `.rs` file under the authority must be
//! listed, so a new file cannot arrive unexamined, and the traversal root
//! is asserted to contain the files it is supposed to reach — narrowing it
//! is a failure, not a silently smaller inventory.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use syn::{Fields, Item, ItemFn, ItemImpl, ItemStruct, Visibility};

/// Every file of the authority, with what it is allowed to expose.
///
/// Listed, not discovered: an unlisted file fails, so the inventory cannot
/// go stale by addition.
const AUTHORITY_FILES: &[&str] = &[
    "alignment.rs",
    "builder.rs",
    "carrier.rs",
    "fences.rs",
    "form.rs",
    "law.rs",
    "minus.rs",
    "mod.rs",
    "pending.rs",
    "port.rs",
    "set.rs",
    "store.rs",
];

/// Types whose every field must be private to the authority.
///
/// A public field is a constructor and a setter at once: it lets a caller
/// build the value from parts and swap a part afterwards.
const SEALED_TYPES: &[&str] = &[
    "SemanticRelation",
    "Interface",
    "ContributionMatrix",
    "SetOutput",
    "ExactHeadingMap",
    "ExactPair",
    "TotalPortMap",
    "Relations",
    "Planning",
    "Vec2",
    "SetStep",
];

/// Opaque identity newtypes: one private field, no public constructor.
const OPAQUE_IDS: &[&str] = &[
    "PortId",
    "ValueId",
    "PaddingId",
    "RelationId",
    "BuilderMark",
    "DefinitionId",
    "StorageId",
];

/// The judgments that must stay total. A wildcard arm in any of them is a
/// place a new form can hide.
const TOTAL_JUDGMENTS: &[&str] = &[
    "law_of",
    "inputs_of",
    "scope_of_export",
    "boundary_of_export",
    "cte_role",
    "cte_label",
    "wrap_of",
    "naming_spelling",
    "output_boundary",
    "proposed_role",
    "prefix_of_definition",
    "ho_role",
    "scratch_prefix",
    "forged_literal",
    "scope_of_form",
    "source_entity",
    "scratch_role",
    "exact_slots",
    "set_step",
    "merge",
    "dependencies_of",
    "storage_of",
    "entity_of",
    "read_source_of",
];

fn authority_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("relation")
}

fn walked_files() -> Vec<PathBuf> {
    let root = authority_root();
    let mut files: Vec<PathBuf> = std::fs::read_dir(&root)
        .expect("the authority directory is readable")
        .map(|entry| entry.expect("directory entry is readable").path())
        .filter(|path| path.extension().is_some_and(|e| e == "rs"))
        .collect();
    files.sort();
    files
}

fn parse(path: &Path) -> syn::File {
    let text = std::fs::read_to_string(path).expect("source file is readable");
    syn::parse_file(&text).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

fn is_public_beyond_authority(vis: &Visibility) -> bool {
    match vis {
        Visibility::Public(_) => true,
        Visibility::Inherited => false,
        // `pub(crate)` reaches the whole compiler; `pub(super)` and
        // `pub(in crate::relation)` do not leave the authority.
        Visibility::Restricted(restricted) => {
            let path = &restricted.path;
            let spelled = quote_path(path);
            spelled == "crate"
        }
    }
}

fn quote_path(path: &syn::Path) -> String {
    path.segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>()
        .join("::")
}

fn items<'a>(file: &'a syn::File) -> Vec<&'a Item> {
    fn walk<'a>(out: &mut Vec<&'a Item>, list: &'a [Item]) {
        for item in list {
            out.push(item);
            if let Item::Mod(module) = item {
                if let Some((_, inner)) = &module.content {
                    walk(out, inner);
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(&mut out, &file.items);
    out
}

fn structs<'a>(file: &'a syn::File) -> Vec<&'a ItemStruct> {
    items(file)
        .into_iter()
        .filter_map(|item| match item {
            Item::Struct(value) => Some(value),
            _ => None,
        })
        .collect()
}

fn functions<'a>(file: &'a syn::File) -> Vec<(&'a ItemFn, bool)> {
    items(file)
        .into_iter()
        .filter_map(|item| match item {
            Item::Fn(value) => Some((value, false)),
            _ => None,
        })
        .collect()
}

fn impls<'a>(file: &'a syn::File) -> Vec<&'a ItemImpl> {
    items(file)
        .into_iter()
        .filter_map(|item| match item {
            Item::Impl(value) => Some(value),
            _ => None,
        })
        .collect()
}

#[test]
fn the_walk_reaches_every_authority_file() {
    let walked: BTreeSet<String> = walked_files()
        .iter()
        .map(|path| {
            path.file_name()
                .expect("a walked path has a file name")
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    let listed: BTreeSet<String> = AUTHORITY_FILES.iter().map(|s| (*s).to_string()).collect();
    let unlisted: Vec<&String> = walked.difference(&listed).collect();
    let missing: Vec<&String> = listed.difference(&walked).collect();
    assert!(
        unlisted.is_empty(),
        "the authority grew files the fence does not examine: {unlisted:?}"
    );
    assert!(
        missing.is_empty(),
        "the fence lists files the authority no longer has, so its traversal \
         root has been narrowed: {missing:?}"
    );
    assert!(
        walked.len() >= AUTHORITY_FILES.len(),
        "the walk returned fewer files than the inventory"
    );
}

#[test]
fn the_carriers_fields_are_private() {
    for path in walked_files() {
        let file = parse(&path);
        for item in structs(&file) {
            let name = item.ident.to_string();
            if !SEALED_TYPES.contains(&name.as_str()) {
                continue;
            }
            for field in &item.fields {
                assert!(
                    !is_public_beyond_authority(&field.vis),
                    "{name} exposes a field outside the authority: a public field \
                     is a constructor and a setter at once"
                );
            }
        }
    }
}

#[test]
fn the_opaque_ids_have_one_private_field() {
    let mut found = BTreeSet::new();
    for path in walked_files() {
        let file = parse(&path);
        for item in structs(&file) {
            let name = item.ident.to_string();
            if !OPAQUE_IDS.contains(&name.as_str()) {
                continue;
            }
            found.insert(name.clone());
            let Fields::Unnamed(fields) = &item.fields else {
                panic!("{name} is an opaque identity and holds one unnamed field");
            };
            assert_eq!(fields.unnamed.len(), 1, "{name} holds exactly one field");
            assert!(
                !is_public_beyond_authority(&fields.unnamed[0].vis),
                "{name}'s payload escapes the authority, so anything holding the \
                 inner value could forge one"
            );
        }
    }
    let listed: BTreeSet<String> = OPAQUE_IDS.iter().map(|s| (*s).to_string()).collect();
    assert_eq!(
        found, listed,
        "an opaque identity in the inventory is not defined in the authority"
    );
}

#[test]
fn no_raw_pairing_constructor_escapes() {
    // The pair has exactly one producer and it is private. A public `new`,
    // `pair`, `of`, `from_parts`, or `From` conversion returning the
    // carrier would be a second road, and a second road is the whole
    // failure mode.
    for path in walked_files() {
        let file = parse(&path);
        for block in impls(&file) {
            let Some(target) = impl_target_name(block) else {
                continue;
            };
            if target != "SemanticRelation" {
                continue;
            }
            assert!(
                block.trait_.is_none()
                    || !matches!(
                        block
                            .trait_
                            .as_ref()
                            .map(|(_, path, _)| quote_path(path))
                            .as_deref(),
                        Some("From") | Some("TryFrom")
                    ),
                "a conversion into SemanticRelation is a road that manufactures \
                 the pair from parts"
            );
            for item in &block.items {
                let syn::ImplItem::Fn(method) = item else {
                    continue;
                };
                if !is_public_beyond_authority(&method.vis) {
                    continue;
                }
                let returns_self = matches!(&method.sig.output, syn::ReturnType::Type(_, ty) if
                matches!(&**ty, syn::Type::Path(p) if {
                    let spelled = quote_path(&p.path);
                    spelled == "Self" || spelled == "SemanticRelation"
                }));
                assert!(
                    !returns_self,
                    "SemanticRelation::{} is a public producer of the pair",
                    method.sig.ident
                );
            }
        }
    }
}

#[test]
fn no_setter_reaches_an_interface_or_an_output_vector() {
    for path in walked_files() {
        let file = parse(&path);
        for block in impls(&file) {
            let Some(target) = impl_target_name(block) else {
                continue;
            };
            if !SEALED_TYPES.contains(&target.as_str()) {
                continue;
            }
            for item in &block.items {
                let syn::ImplItem::Fn(method) = item else {
                    continue;
                };
                let name = method.sig.ident.to_string();
                let takes_mut_self = matches!(
                    method.sig.inputs.first(),
                    Some(syn::FnArg::Receiver(receiver)) if receiver.mutability.is_some()
                );
                assert!(
                    !(takes_mut_self && is_public_beyond_authority(&method.vis)),
                    "{target}::{name} mutates a sealed carrier from outside the \
                     authority"
                );
                assert!(
                    !name.starts_with("set_"),
                    "{target}::{name} is a setter on a carrier whose parts are \
                     the authority's to decide"
                );
            }
        }
    }
}

#[test]
fn the_total_judgments_have_no_wildcard_arm() {
    let mut seen = BTreeSet::new();
    for path in walked_files() {
        let text = std::fs::read_to_string(&path).expect("source file is readable");
        let file = parse(&path);
        let mut check = |name: String, body: &syn::Block| {
            if !TOTAL_JUDGMENTS.contains(&name.as_str()) {
                return;
            }
            seen.insert(name.clone());
            assert!(
                !block_has_wildcard_arm(body),
                "{name} has a wildcard match arm, so a new form can take an \
                 answer nobody wrote for it"
            );
        };
        for (function, _) in functions(&file) {
            check(function.sig.ident.to_string(), &function.block);
        }
        for block in impls(&file) {
            for item in &block.items {
                if let syn::ImplItem::Fn(method) = item {
                    check(method.sig.ident.to_string(), &method.block);
                }
            }
        }
        // Assembled at runtime so this file does not match itself.
        let open_marker = String::from("#[non_") + "exhaustive]";
        assert!(
            !text.contains(&open_marker),
            "{}: a closed semantic vocabulary marked open forces wildcard \
             handling at every consumer",
            path.display()
        );
    }
    let listed: BTreeSet<String> = TOTAL_JUDGMENTS.iter().map(|s| (*s).to_string()).collect();
    let missing: Vec<&String> = listed.difference(&seen).collect();
    assert!(
        missing.is_empty(),
        "a judgment the fence guards no longer exists, so the guard is \
         watching nothing: {missing:?}"
    );
}

/// A wildcard arm is `_` or a binding that matches everything. A struct or
/// tuple pattern with `..` inside it is not a wildcard ARM — it ignores
/// fields of a variant it already named.
fn block_has_wildcard_arm(block: &syn::Block) -> bool {
    struct Finder {
        found: bool,
    }
    impl<'ast> syn::visit::Visit<'ast> for Finder {
        fn visit_expr_match(&mut self, node: &'ast syn::ExprMatch) {
            for arm in &node.arms {
                if is_catch_all(&arm.pat) {
                    self.found = true;
                }
            }
            syn::visit::visit_expr_match(self, node);
        }
    }
    fn is_catch_all(pattern: &syn::Pat) -> bool {
        match pattern {
            syn::Pat::Wild(_) => true,
            // A bare identifier is a BINDING only when it is not a unit
            // variant. `syn` cannot tell `None` from `whatever` without
            // resolution; the case convention can.
            syn::Pat::Ident(ident) => {
                ident.subpat.is_none()
                    && ident.by_ref.is_none()
                    && !ident
                        .ident
                        .to_string()
                        .starts_with(|first: char| first.is_uppercase())
            }
            syn::Pat::Or(or) => or.cases.iter().any(is_catch_all),
            _ => false,
        }
    }
    let mut finder = Finder { found: false };
    syn::visit::Visit::visit_block(&mut finder, block);
    finder.found
}

/// A struct-literal expression naming one of the sealed carriers.
fn forged_literal(file: &syn::File) -> Option<String> {
    struct Finder {
        found: Option<String>,
    }
    impl<'ast> syn::visit::Visit<'ast> for Finder {
        fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
            if let Some(last) = node.path.segments.last() {
                let name = last.ident.to_string();
                // `Vec2` is a shape, not a carrier: the name is generic
                // enough that another module may have its own, and its
                // fields are already proved private.
                if name != "Vec2" && SEALED_TYPES.contains(&name.as_str()) {
                    self.found.get_or_insert(name);
                }
            }
            syn::visit::visit_expr_struct(self, node);
        }
    }
    let mut finder = Finder { found: None };
    syn::visit::Visit::visit_file(&mut finder, file);
    finder.found
}

/// The written spelling of a type, for comparing a signature against what
/// it is supposed to be.
/// Whether a struct DERIVES one trait.
fn derives(item: &ItemStruct, want: &str) -> bool {
    let mut found = false;
    for attribute in &item.attrs {
        if !attribute.path().is_ident("derive") {
            continue;
        }
        let _ = attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident(want) {
                found = true;
            }
            Ok(())
        });
    }
    found
}

/// Whether an inherent impl is written for the phase that carries NO
/// relation — `impl<P: Phase<Scope = ()>> …`.
///
/// Read off the bound's own associated-type binding rather than off the
/// text: a fence that grepped for the characters would pass the moment
/// someone reformatted the line.
fn binds_scope_to_unit(item: &ItemImpl) -> bool {
    for param in &item.generics.params {
        let syn::GenericParam::Type(param) = param else {
            continue;
        };
        for bound in &param.bounds {
            let syn::TypeParamBound::Trait(bound) = bound else {
                continue;
            };
            let Some(last) = bound.path.segments.last() else {
                continue;
            };
            let syn::PathArguments::AngleBracketed(arguments) = &last.arguments else {
                continue;
            };
            for argument in &arguments.args {
                let syn::GenericArgument::AssocType(assoc) = argument else {
                    continue;
                };
                if assoc.ident == "Scope"
                    && matches!(&assoc.ty, syn::Type::Tuple(t) if t.elems.is_empty())
                {
                    return true;
                }
            }
        }
    }
    false
}

/// The return type of one signature, as a path name.
fn returns(sig: &syn::Signature) -> String {
    match &sig.output {
        syn::ReturnType::Default => String::new(),
        syn::ReturnType::Type(_, ty) => quote_path_of_type(ty),
    }
}

fn quote_path_of_type(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Path(path) => quote_path(&path.path),
        syn::Type::Reference(reference) => format!("&{}", quote_path_of_type(&reference.elem)),
        _ => String::new(),
    }
}

fn impl_target_name(block: &ItemImpl) -> Option<String> {
    match &*block.self_ty {
        syn::Type::Path(path) => path.path.segments.last().map(|s| s.ident.to_string()),
        _ => None,
    }
}

#[test]
fn only_the_authority_constructs_the_carrier() {
    // Outside `src/relation`, no production file may name the pairing
    // constructor or the identity newtypes' payloads. The visibility
    // already refuses it; this proves the visibility was not widened.
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let authority = authority_root();
    let mut offenders = Vec::new();
    let mut walked = 0usize;
    let walk = |dir: &Path, out: &mut Vec<String>, count: &mut usize| {
        fn recurse(dir: &Path, out: &mut Vec<String>, count: &mut usize, authority: &Path) {
            for entry in std::fs::read_dir(dir).expect("source tree is readable") {
                let path = entry.expect("directory entry is readable").path();
                if path.is_dir() {
                    recurse(&path, out, count, authority);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    if path.starts_with(authority) {
                        continue;
                    }
                    *count += 1;
                    let text = std::fs::read_to_string(&path).expect("source file is readable");
                    for needle in [
                        "SemanticRelation::pair(",
                        "Interface::of(",
                        "Interface::opaque(",
                    ] {
                        if text.contains(needle) {
                            out.push(format!("{}: {needle}", path.display()));
                        }
                    }
                    // A struct LITERAL, not a type in a signature: parsing
                    // is what tells `SemanticRelation { … }` apart from
                    // `-> SemanticRelation {`.
                    let file = syn::parse_file(&text)
                        .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
                    if let Some(name) = forged_literal(&file) {
                        out.push(format!("{}: {name} literal", path.display()));
                    }
                }
            }
        }
        recurse(dir, out, count, &authority);
    };
    walk(&src, &mut offenders, &mut walked);
    // The floor is a FAIL-CLOSED check on the traversal, not a census: a
    // root that stops resolving submodules returns a fraction of the tree,
    // and a fraction that still finds nothing is not evidence.
    assert!(
        walked >= 250,
        "the source walk reached only {walked} files, so its root has been \
         narrowed"
    );
    assert!(
        offenders.is_empty(),
        "the semantic carrier is constructed outside its authority: {offenders:?}"
    );
}

/// The modules past the semantic epoch, and the ones still inside it.
///
/// Sealing is STRUCTURAL: what stops a lowering from minting is that it
/// never builds an authority. The listed survivors are the residue — plan
/// construction that still reaches the entrance from a transformer file —
/// and each has a boundary that deletes it.
const SEALED_ROOTS: &[&str] = &[
    "pipeline/transformer",
    "pipeline/sql_ast",
    "pipeline/sql_rewriter",
    "pipeline/generator",
];

/// Files under a sealed root that still name the capability.
///
/// Empty, and the type is what keeps it so: a lowering context is handed
/// `Rc<Registry>` and `Relations`, and there is no road from either to the
/// capability. This walk is the RECEIPT that nobody threaded one in anyway.
const SEAL_SURVIVORS: &[&str] = &[];

#[test]
fn lowering_holds_no_construction_capability() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    let mut walked = 0usize;
    fn recurse(dir: &Path, src: &Path, out: &mut Vec<String>, count: &mut usize) {
        for entry in std::fs::read_dir(dir).expect("source tree is readable") {
            let path = entry.expect("directory entry is readable").path();
            if path.is_dir() {
                recurse(&path, src, out, count);
                continue;
            }
            if !path.extension().is_some_and(|e| e == "rs") {
                continue;
            }
            let rel = path
                .strip_prefix(src)
                .expect("walked path is under src")
                .to_string_lossy()
                .replace('\\', "/");
            if !SEALED_ROOTS.iter().any(|root| rel.starts_with(root)) {
                continue;
            }
            *count += 1;
            if SEAL_SURVIVORS.contains(&rel.as_str()) {
                continue;
            }
            let text = std::fs::read_to_string(&path).expect("source file is readable");
            let capability = String::from("Plan") + "ning";
            let builder = String::from("Semantic") + "Builder";
            if text.contains(&capability) || text.contains(&builder) {
                out.push(rel);
            }
        }
    }
    recurse(&src, &src, &mut offenders, &mut walked);
    assert!(
        walked >= 30,
        "the lowering walk reached only {walked} files, so its roots have \
         been narrowed"
    );
    assert!(
        offenders.is_empty(),
        "a module past the semantic epoch can construct a relation: {offenders:?}"
    );
}

/// THE CAPABILITY HAS ONE PRODUCER, AND IT TAKES THE REGISTRY BY VALUE.
///
/// This is the whole sealing fence. A phase that constructs holds a
/// `Planning`; a phase that lowers holds `Rc<Registry>` and `Relations`.
/// Because the one producer consumes an OWNED registry, a shared handle
/// cannot be turned back into the capability — so lowering cannot recover a
/// builder from what it holds, and a future edit cannot restore
/// transformer-side construction by reaching for the registry already in
/// scope.
#[test]
fn the_capability_is_produced_only_from_an_owned_registry() {
    let mut producers: Vec<(String, Vec<String>)> = Vec::new();
    for path in walked_files() {
        let file = parse(&path);
        for block in impls(&file) {
            let on_planning = impl_target_name(block).as_deref() == Some("Planning");
            for item in &block.items {
                let syn::ImplItem::Fn(function) = item else {
                    continue;
                };
                let returns = match &function.sig.output {
                    syn::ReturnType::Type(_, ty) => quote_path_of_type(ty),
                    syn::ReturnType::Default => String::new(),
                };
                let produces = returns.contains("Planning") || (on_planning && returns == "Self");
                if !produces {
                    continue;
                }
                producers.push((
                    function.sig.ident.to_string(),
                    function
                        .sig
                        .inputs
                        .iter()
                        .map(|argument| match argument {
                            syn::FnArg::Typed(typed) => quote_path_of_type(&typed.ty),
                            syn::FnArg::Receiver(_) => "self".to_string(),
                        })
                        .collect(),
                ));
            }
        }
        for (function, _) in functions(&file) {
            let returns = match &function.sig.output {
                syn::ReturnType::Type(_, ty) => quote_path_of_type(ty),
                syn::ReturnType::Default => String::new(),
            };
            if returns.contains("Planning") {
                producers.push((function.sig.ident.to_string(), Vec::new()));
            }
        }
    }
    assert_eq!(
        producers.len(),
        1,
        "the open semantic epoch has more than one producer: {producers:?}"
    );
    assert_eq!(producers[0].0, "open", "the producer was renamed");
    assert_eq!(
        producers[0].1,
        vec!["crate::names::Registry".to_string()],
        "the capability's producer takes something other than an owned \
         registry, so a shared handle can be turned back into it: {:?}",
        producers[0].1
    );
}

/// The builder itself is unnameable outside the authority.
#[test]
fn the_builder_is_not_constructible_outside_the_authority() {
    let file = parse(&authority_root().join("builder.rs"));
    let mut checked = 0usize;
    for block in impls(&file) {
        if impl_target_name(block).as_deref() != Some("SemanticBuilder") {
            continue;
        }
        for item in &block.items {
            let syn::ImplItem::Fn(function) = item else {
                continue;
            };
            if function.sig.ident != "new" {
                continue;
            }
            checked += 1;
            assert!(
                !is_public_beyond_authority(&function.vis),
                "SemanticBuilder::new is reachable outside the authority, so \
                 a bare registry is a construction capability again"
            );
        }
    }
    assert_eq!(checked, 1, "SemanticBuilder::new was not found to check");
}

#[test]
fn the_carrier_has_one_producer_inside_the_authority() {
    // The visibility already confines the pairing constructor to
    // `crate::relation`; this says WHERE inside it. One producer means the
    // record and the carrier are written in one act, so no road hands back
    // a carrier for a relation the store never recorded.
    let mut offenders = Vec::new();
    for path in walked_files() {
        let name = path
            .file_name()
            .expect("a walked path has a file name")
            .to_string_lossy()
            .into_owned();
        if name == "store.rs" || name == "fences.rs" {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("source file is readable");
        if text.contains(&(String::from("SemanticRelation::") + "pair(")) {
            offenders.push(name);
        }
    }
    assert!(
        offenders.is_empty(),
        "a second file produces a carrier without recording it: {offenders:?}"
    );
}

/// The temporary roads that survive, and how many mentions each still has.
///
/// A RATCHET, not a census. Every entry is a place the architecture has not
/// yet taken over, counted so the migration can only shrink it: raising a
/// number takes an edit here, and the edit is where a reviewer asks why a
/// temporary road grew a caller.
///
/// Two kinds live here. A PREDECESSOR road is one the authority has not yet
/// replaced. A BRIDGE is one a later U-step deletes outright rather than
/// replaces — it exists because the architecture that answers its question
/// by construction does not exist yet.
const SURVIVING_PREDECESSOR_ROADS: &[(&str, usize)] = &[
    // The occurrence road: the operation re-stages and publishes its own
    // positions afterwards. Closed for the set family; open elsewhere.
    // The unary-publication road: projection, aggregation and reduction
    // mint their own output columns and name the position they minted.
];

/// THE AST CARRIES NO PUBLIC SEMANTIC RESULT.
///
/// A relation attachable to any valid node is a relation attachable to the
/// WRONG one. Every position of a chain that publishes — the head and each
/// step — is a struct with a private result, so the only road from a
/// relation to a node is the authority's, which derives the relation from
/// that node's own form in one act.
///
/// The check is over the AST's own source rather than the authority's: an
/// enum variant's fields are as public as the enum, so a `result` field on
/// a continuation variant is a construction road no visibility could close
/// — which is exactly why the result lives on the pair instead.
#[test]
fn no_ast_node_carries_a_public_semantic_result() {
    let ast = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/pipeline/asts/core");
    let mut offenders = Vec::new();
    let mut walked = 0usize;
    fn recurse(dir: &Path, out: &mut Vec<String>, count: &mut usize) {
        for entry in std::fs::read_dir(dir).expect("the AST source is readable") {
            let path = entry.expect("directory entry is readable").path();
            if path.is_dir() {
                recurse(&path, out, count);
                continue;
            }
            if !path.extension().is_some_and(|e| e == "rs") {
                continue;
            }
            *count += 1;
            let file = parse(&path);
            for item in structs(&file) {
                for field in item.fields.iter() {
                    let Some(name) = field.ident.as_ref() else {
                        continue;
                    };
                    let named = name.to_string();
                    if named != "result" && named != "scoped" {
                        continue;
                    }
                    // The phase's RELATION payload, not a scalar arm's
                    // value: `P::Scope` is what a row-producing node
                    // publishes, and only that is a semantic result.
                    if !quote_path_of_type(&field.ty).contains("Scope") {
                        continue;
                    }
                    if is_public_beyond_authority(&field.vis) {
                        out.push(format!("{}::{named}", item.ident));
                    }
                }
            }
            // An enum variant's fields cannot be private, so a semantic
            // result on one is a public attachment road by construction.
            for item in items(&file) {
                let Item::Enum(item) = item else { continue };
                for variant in &item.variants {
                    for field in variant.fields.iter() {
                        let Some(name) = field.ident.as_ref() else {
                            continue;
                        };
                        let named = name.to_string();
                        if (named == "result" || named == "scoped")
                            && quote_path_of_type(&field.ty).contains("Scope")
                        {
                            out.push(format!("{}::{}::{named}", item.ident, variant.ident));
                        }
                    }
                }
            }
        }
    }
    recurse(&ast, &mut offenders, &mut walked);
    assert!(
        walked >= 20,
        "the AST walk reached only {walked} files, so its root has been narrowed"
    );
    assert!(
        offenders.is_empty(),
        "a node carries a semantic result a caller can attach or replace: {offenders:?}"
    );
}

/// REFINEMENT REPORTS ITS OUTCOME; IT DOES NOT JUDGE IT AFTERWARDS.
///
/// The authority's refinement road decides what a refinement DID from the
/// CONSTRUCTION RECORD. A published spelling, an addressing, or a position
/// index appearing in that decision would be a lineage nobody wrote down
/// being manufactured from characters, which is the shape this cut
/// deleted.
#[test]
fn the_refinement_outcome_reads_no_spelling_and_no_ordinal() {
    let text = std::fs::read_to_string(authority_root().join("builder.rs"))
        .expect("the authority's source is readable");
    let start = text
        .find("fn replacement_made")
        .expect("the refinement judgment is present");
    let end = text[start..]
        .find("\n    /// Where one operand port landed")
        .map(|at| start + at)
        .unwrap_or(text.len());
    let road = &text[start..end];
    for needle in ["published(", "addressing(", "published_sym(", ".zip("] {
        assert!(
            !road.contains(needle),
            "the refinement outcome consults `{needle}`, which is a character \
             or a position rather than an edge construction recorded"
        );
    }
}

/// THE CAPABILITY IS NOT COPYABLE.
///
/// `Planning` deriving or implementing `Clone` would undo the whole
/// transition: a capability that can be copied cannot be spent, and
/// sealing a copy leaves the original open beside the reader it produced.
/// The seal takes `self`; this is what keeps that from being a formality.
#[test]
fn the_capability_is_not_copyable() {
    let file = parse(&authority_root().join("mod.rs"));
    let mut checked = 0usize;
    for item in structs(&file) {
        if item.ident != "Planning" {
            continue;
        }
        checked += 1;
        assert!(
            !derives(item, "Clone"),
            "the construction capability derives Clone, so sealing a copy \
             leaves the original open"
        );
    }
    assert_eq!(checked, 1, "Planning was not found to check");
    for path in walked_files() {
        let file = parse(&path);
        for item in impls(&file) {
            let Some((_, trait_path, _)) = item.trait_.as_ref() else {
                continue;
            };
            if !quote_path(trait_path).ends_with("Clone") {
                continue;
            }
            assert!(
                impl_target_name(item).as_deref() != Some("Planning"),
                "the construction capability implements Clone by hand"
            );
        }
    }
}

/// A BOUND NODE'S PAYLOAD HAS NO SETTER AND NO FREE REWRITE.
///
/// `form_mut` and `rewrite_form` exist only where the phase has no
/// relation to mispair (`Scope = ()`). A bound one would swap what a node
/// DOES while what it publishes stays — two individually valid objects
/// related wrongly by a call-site choice, which is the failure class this
/// arc exists to remove.
#[test]
fn a_bound_node_has_no_payload_setter() {
    let chain =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/pipeline/asts/core/expressions/chain.rs");
    let file = parse(&chain);
    let mut found = 0usize;
    for item in impls(&file) {
        if item.trait_.is_some() {
            continue;
        }
        let authored = binds_scope_to_unit(item);
        for member in &item.items {
            let syn::ImplItem::Fn(method) = member else {
                continue;
            };
            let name = method.sig.ident.to_string();
            if name != "form_mut" && name != "rewrite_form" && name != "then" {
                continue;
            }
            found += 1;
            assert!(
                authored,
                "`{name}` is reachable in a phase that carries a relation, so a \
                 payload can be swapped or a step appended while what the node \
                 publishes stays"
            );
        }
    }
    assert!(
        found >= 3,
        "the authored-phase mutation roads were not found to check"
    );
}

/// NO PUBLICATION POSITION CARRIES A WRITABLE OUTPUT PORT.
///
/// A port any caller can write is a port any caller can CHOOSE, and
/// choosing which occurrence a position stands at is the authority's act.
#[test]
fn no_publication_position_has_a_public_output() {
    let specs = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/pipeline/asts/core/specs.rs");
    let file = parse(&specs);
    let mut checked = 0usize;
    for item in structs(&file) {
        let name = item.ident.to_string();
        if !matches!(name.as_str(), "OneOut" | "MetadataOut" | "NamedOutItem") {
            continue;
        }
        checked += 1;
        for field in item.fields.iter() {
            let Some(field_name) = field.ident.as_ref() else {
                continue;
            };
            if field_name != "output" {
                continue;
            }
            assert!(
                matches!(field.vis, Visibility::Inherited),
                "{name}'s published position is writable from outside its carrier"
            );
        }
    }
    assert_eq!(checked, 3, "the publication carriers were not all found");
}

/// A REFINEMENT OUTCOME HAS NO ERASER.
///
/// A road that hands back the node whichever arm it is lets a caller walk
/// past "a rebuild happened here" — and a rebuild's map is what lowering
/// translates an old port through. Every arm must be reached by matching.
#[test]
fn the_refinement_outcome_has_no_eraser() {
    let file = parse(&authority_root().join("builder.rs"));
    for item in impls(&file) {
        if item.trait_.is_some() {
            continue;
        }
        if impl_target_name(item).as_deref() != Some("Refinement") {
            continue;
        }
        for member in &item.items {
            let syn::ImplItem::Fn(method) = member else {
                continue;
            };
            let answer = returns(&method.sig);
            assert!(
                !answer.contains("Chain"),
                "the refinement outcome has a road that answers with the node \
                 whichever arm it is: {}",
                method.sig.ident
            );
        }
    }
}

/// A QUALIFIER IS NOT ASKED WHETHER IT EMITS.
///
/// `Qualify` carrying an `Option<SqlSiteId>` made "maybe there is a site
/// under this" the universal interface, and every road below it had to
/// turn the absence into a refusal of its own. Emission is a PROPERTY:
/// `Emitting` carries the site as a field, and a scope that emits nothing
/// simply is not one.
#[test]
fn no_qualifier_answers_maybe_it_emits() {
    let builder =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/pipeline/transformer/builder/mod.rs");
    let text = std::fs::read_to_string(&builder).expect("the builder's source is readable");
    assert!(
        !text.contains("fn sql_site(&self) -> Option<"),
        "the qualifier interface still asks every scope whether it has a site"
    );
    let file = parse(&builder);
    let mut checked = 0usize;
    for item in items(&file) {
        let Item::Trait(declared) = item else {
            continue;
        };
        if declared.ident != "Emitting" {
            continue;
        }
        checked += 1;
        for member in &declared.items {
            let syn::TraitItem::Fn(method) = member else {
                continue;
            };
            assert!(
                !returns(&method.sig).contains("Option"),
                "an emitting qualifier's site is optional: {}",
                method.sig.ident
            );
        }
    }
    assert_eq!(checked, 1, "the Emitting trait was not found to check");
}

/// EVERY EMITTED SELECT POSITION STATES WHAT IT REALIZES.
///
/// `SelectItem` used to carry `alias: Option<ColId>`, and readers
/// recovered the position's identity from the expression's SHAPE — an
/// item with no alias whose expression happened to be a column silently
/// WAS that column's position. The identity is a field now, and whether
/// SQL prints an `AS` is a separate, stated decision.
#[test]
fn no_select_position_hides_its_identity() {
    let items_file =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/pipeline/sql_ast/select_items.rs");
    let file = parse(&items_file);
    let mut checked = 0usize;
    for item in items(&file) {
        let Item::Enum(declared) = item else {
            continue;
        };
        if declared.ident != "SelectItem" {
            continue;
        }
        checked += 1;
        for variant in &declared.variants {
            for field in variant.fields.iter() {
                let Some(name) = field.ident.as_ref() else {
                    continue;
                };
                if name != "slot" {
                    continue;
                }
                assert!(
                    !quote_path_of_type(&field.ty).contains("Option"),
                    "an emitted select position can be built without the \
                     occurrence it realizes"
                );
            }
        }
    }
    assert_eq!(checked, 1, "SelectItem was not found to check");
}

/// EVERY EMITTED SQL LAYER HAS ONE PHYSICAL SLOT IDENTITY.
///
/// `SqlLayout` is where a lowering says what a level emits. Its site is
/// bound where the layout is built, so "not yet bound" is not a state the
/// type has and no later reader checks for one.
#[test]
fn a_sql_layout_has_no_unbound_state() {
    let layout =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/pipeline/transformer/builder/layout.rs");
    let file = parse(&layout);
    let mut checked = 0usize;
    for item in structs(&file) {
        if item.ident != "SqlLayout" {
            continue;
        }
        checked += 1;
        for field in item.fields.iter() {
            let Some(name) = field.ident.as_ref() else {
                continue;
            };
            if name != "site" {
                continue;
            }
            assert!(
                !is_public_beyond_authority(&field.vis),
                "a SQL layout's site is writable from outside the layout"
            );
            assert!(
                !quote_path_of_type(&field.ty).contains("Option"),
                "a SQL layout can be built without a physical slot identity"
            );
        }
    }
    assert_eq!(checked, 1, "SqlLayout was not found to check");
}

#[test]
fn the_predecessor_roads_only_shrink() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    fn recurse(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("source tree is readable") {
            let path = entry.expect("directory entry is readable").path();
            if path.is_dir() {
                recurse(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    recurse(&src, &mut files);
    // The declarations live inside the authority and are not callers.
    let authority = authority_root();
    let mut drift = Vec::new();
    for (needle, expected) in SURVIVING_PREDECESSOR_ROADS {
        let mut found = 0usize;
        for path in &files {
            if path.starts_with(&authority) {
                continue;
            }
            let text = std::fs::read_to_string(path).expect("source file is readable");
            found += text.matches(needle).count();
        }
        if found != *expected {
            drift.push(format!("{needle}: {expected} -> {found}"));
        }
    }
    assert!(
        drift.is_empty(),
        "a temporary road's mention count moved; if it SHRANK, record the \
         new number here — if it grew, the architecture lost ground:\n  {}",
        drift.join("\n  ")
    );
}

/// The functions that emit a set operation.
///
/// One list, so a road that grows a second alignment judgment has to be
/// added here before it can hide.
const SET_LOWERING_ROAD: &[&str] = &[
    "r_lower_set_op",
    "r_lower_correlated_set_op",
    "r_lower_intersect_min_multiplicity",
    "align_arm_items",
];

/// The judgments a set-emitting function may not reach.
///
/// Each decides which of two positions stands for which — the question the
/// contribution matrix answered at construction and the physical binding
/// answered at layout. A function that could call one could answer it a
/// third time, and differently.
const FORBIDDEN_JUDGMENTS: &[&str] = &[
    "corresponding_slots",
    "stable_name_alignment",
    "published_sym",
    "value_class",
    "descendant",
];

/// THE MUTATION ARGUMENT, made structurally.
///
/// A recorded set's emitted shape cannot change when a correspondence
/// judgment changes, because no function on the road that emits one calls
/// any of them. The alignment comes from the table the authority recorded
/// and the physical column from the binding the branches were laid out
/// under; these decide nothing here, so mutating them decides nothing here.
///
/// Checked by walking calls rather than by counting text: a call is what
/// makes an authority reachable, and a mention in a comment is not one.
#[test]
fn the_set_lowering_road_calls_no_correspondence_matcher() {
    use syn::visit::Visit;

    struct Calls {
        inside: Option<String>,
        offenders: Vec<String>,
        seen: BTreeSet<String>,
    }

    impl Calls {
        fn enter(&mut self, name: String, block: &syn::Block) {
            if !SET_LOWERING_ROAD.contains(&name.as_str()) {
                return;
            }
            self.seen.insert(name.clone());
            let previous = self.inside.replace(name);
            syn::visit::visit_block(self, block);
            self.inside = previous;
        }

        fn reached(&mut self, called: &syn::Ident) {
            if let Some(inside) = &self.inside {
                if FORBIDDEN_JUDGMENTS.iter().any(|name| called == name) {
                    self.offenders.push(format!("{inside} -> {called}"));
                }
            }
        }
    }

    impl<'ast> Visit<'ast> for Calls {
        fn visit_item_fn(&mut self, item: &'ast ItemFn) {
            self.enter(item.sig.ident.to_string(), &item.block);
        }

        fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
            self.reached(&call.method);
            syn::visit::visit_expr_method_call(self, call);
        }

        fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
            if let syn::Expr::Path(path) = call.func.as_ref() {
                if let Some(last) = path.path.segments.last() {
                    self.reached(&last.ident);
                }
            }
            syn::visit::visit_expr_call(self, call);
        }
    }

    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("pipeline")
        .join("transformer")
        .join("relational.rs");
    let text = std::fs::read_to_string(&path).expect("the lowering source is readable");
    let file = syn::parse_file(&text).expect("the lowering source parses");
    let mut calls = Calls {
        inside: None,
        offenders: Vec::new(),
        seen: BTreeSet::new(),
    };
    calls.visit_file(&file);

    let listed: BTreeSet<String> = SET_LOWERING_ROAD.iter().map(|s| (*s).to_string()).collect();
    let missing: Vec<&String> = listed.difference(&calls.seen).collect();
    assert!(
        missing.is_empty(),
        "the fence names functions the set lowering road no longer has, so it \
         is watching nothing: {missing:?}"
    );
    assert!(
        calls.offenders.is_empty(),
        "a set-emitting function reaches a correspondence judgment, beside the \
         table the authority recorded and the binding its branches were laid \
         out under: {:?}",
        calls.offenders
    );
}
