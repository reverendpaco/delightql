// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The minted build-identity facts: the grammar fingerprint is the digest of
//! THIS build's generated artifacts under the length framing, and each framed
//! artifact participates — perturbing either one moves the digest.

use sha2::{Digest, Sha256};
use std::path::PathBuf;

fn generated_artifacts() -> (Vec<u8>, Vec<u8>) {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .join("grammar/src");
    (
        std::fs::read(src.join("parser.c")).expect("parser.c exists after a build"),
        std::fs::read(src.join("node-types.json")).expect("node-types.json exists after a build"),
    )
}

fn framed_digest(parser_c: &[u8], node_types: &[u8]) -> String {
    let mut hasher = Sha256::new();
    for artifact in [parser_c, node_types] {
        hasher.update((artifact.len() as u64).to_be_bytes());
        hasher.update(artifact);
    }
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The exported constant IS the framed digest of the artifacts this build
/// generated — not a stale copy, not a digest of something else.
#[test]
fn fingerprint_is_the_framed_digest_of_this_builds_artifacts() {
    let (parser_c, node_types) = generated_artifacts();
    assert_eq!(
        delightql_cst::GRAMMAR_FINGERPRINT,
        framed_digest(&parser_c, &node_types),
        "the exported fingerprint must be the framed SHA-256 of the current artifacts"
    );
    assert_eq!(delightql_cst::GRAMMAR_FINGERPRINT.len(), 64);
}

/// Either artifact changing changes the fingerprint, and the framing keeps
/// the pair unambiguous: moving a byte across the seam is a different digest.
#[test]
fn each_framed_artifact_moves_the_fingerprint() {
    let (parser_c, node_types) = generated_artifacts();
    let baseline = framed_digest(&parser_c, &node_types);

    let mut parser_touched = parser_c.clone();
    parser_touched.push(b'x');
    assert_ne!(baseline, framed_digest(&parser_touched, &node_types));

    let mut types_touched = node_types.clone();
    types_touched.push(b'x');
    assert_ne!(baseline, framed_digest(&parser_c, &types_touched));

    // The seam: shifting one byte from the head of the second artifact onto
    // the tail of the first preserves the concatenation but not the digest.
    if let Some((&first, rest)) = node_types.split_first() {
        let mut shifted_left = parser_c.clone();
        shifted_left.push(first);
        assert_ne!(baseline, framed_digest(&shifted_left, rest));
    }
}

/// The runtime fact names the one parser runtime this workspace links, and
/// that runtime IS the generator pin: the relationship, read from the
/// Makefile and the lockfile the way the build script reads them, not a
/// literal this file would have to be edited to keep true.
#[test]
fn parser_runtime_names_the_linked_runtime() {
    let version = delightql_cst::PARSER_RUNTIME
        .strip_prefix("tree-sitter ")
        .unwrap_or_else(|| panic!("unexpected runtime spelling: {}", delightql_cst::PARSER_RUNTIME));
    let root = workspace_root();
    let makefile = std::fs::read_to_string(root.join("Makefile")).expect("the Makefile");
    let pin = tuple::makefile_assignment(&makefile, "TREE_SITTER_EXPECTED_VERSION")
        .expect("the Makefile pins the generator");
    let lock = std::fs::read_to_string(root.join("Cargo.lock")).expect("the lockfile");
    let facts = tuple::TupleFacts {
        generator_pin: pin,
        runtimes: tuple::lockfile_versions(&lock, "tree-sitter"),
        highlighters: tuple::lockfile_versions(&lock, "tree-sitter-highlight"),
    };
    assert_eq!(tuple::judge(&facts).as_deref(), Ok(version), "{facts:?}");
    assert!(
        (tree_sitter::MIN_COMPATIBLE_LANGUAGE_VERSION..=tree_sitter::LANGUAGE_VERSION)
            .contains(&delightql_cst::language().abi_version()),
        "the generated parser's ABI is one the linked runtime reads"
    );
}

/// The judgment the build script applies, with each side of the
/// relationship perturbed alone: the falsifiers live beside the authority
/// (`generator/tuple.rs`), and this target compiles them.
#[path = "../generator/tuple.rs"]
#[allow(dead_code)]
mod tuple;

fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("the workspace root")
        .to_path_buf()
}
