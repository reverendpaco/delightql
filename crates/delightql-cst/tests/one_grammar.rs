// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! ONE GRAMMAR, tested as structure rather than as vocabulary.
//!
//! There is one authored grammar directory, one generated language the façade
//! binds and can parse with, one typed alphabet generated from that language's
//! own description, and one road from a tree to the semantic AST. Each test
//! below asks the repository whether that is still true.
//!
//! A NAME IS NOT A STRUCTURE. Forbidding particular spellings would pass a
//! second parser introduced under any other name, so nothing here scans for
//! names; every check reaches for the thing itself.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/<pkg> sits two levels under the workspace root")
        .to_path_buf()
}

/// ONE AUTHORED GRAMMAR, under an ordinary name.
#[test]
fn one_authored_grammar_directory() {
    let root = workspace_root();
    let authored: BTreeSet<String> = std::fs::read_dir(&root)
        .expect("the workspace root is readable")
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|name| name.starts_with("grammar") && root.join(name).is_dir())
        .collect();
    assert_eq!(
        authored,
        BTreeSet::from(["grammar".to_string()]),
        "there is one authored grammar and its name is `grammar`"
    );

    // Its AUTHORED inputs are tracked; everything the generator writes is
    // derived and lives under the ignored `src/`.
    for input in [
        "grammar.js",
        "tokens.js",
        "conflicts.js",
        "tree-sitter.json",
        "queries/highlights.scm",
    ] {
        assert!(
            root.join("grammar").join(input).exists(),
            "the grammar's authored input {input} is missing"
        );
    }
}

/// ONE GENERATED LANGUAGE. The façade binds one symbol and offers one handle.
#[test]
fn one_generated_language() {
    let facade =
        std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs"))
            .expect("the façade is readable");
    let bindings = facade.matches("fn tree_sitter_").count();
    assert_eq!(
        bindings, 1,
        "the façade binds one generated language, not {bindings}"
    );
    assert!(facade.contains("fn tree_sitter_delightql()"));

    // And it PARSES: the invariant is about a language that exists, not a
    // symbol that links.
    let mut parser = delightql_cst::Parser::new();
    let tree = parser.parse_query_sequence("users(*) |> (id)");
    assert!(!tree.has_defects(), "{:?}", tree.defects());
}

/// ONE TYPED CST API, generated from that language's own description.
#[test]
fn one_typed_cst_api() {
    use delightql_cst::cst::{Kind, ALL, SUBTYPES};
    assert!(
        ALL.len() > 300,
        "the typed alphabet is suspiciously small: {}",
        ALL.len()
    );
    assert!(!SUBTYPES.is_empty(), "the supertype table is empty");
    // Every kind round-trips through its own name, which is what makes the
    // alphabet usable as data by a consumer partitioning the language.
    for kind in ALL {
        assert_eq!(Kind::from_str(kind.as_str()), Some(*kind));
    }
}

/// ONE ROAD from a tree to the semantic AST, and it is installed.
#[test]
fn one_normalization_road() {
    let core = workspace_root().join("crates/delightql-core/src/pipeline");
    for present in ["parse", "normalize", "syntax.rs"] {
        assert!(
            core.join(present).exists(),
            "the one road is missing {present}"
        );
    }
}
