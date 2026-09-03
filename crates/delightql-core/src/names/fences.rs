// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Source fences over the identity surface.
//!
//! The identity-hardening ratchet: every file that spells a raw scope
//! mint, a baptism hint constructor, or a progenitor-equality call is
//! pinned here with its exact occurrence count, whole file, test
//! modules included. A count that moves — either direction — fails, so
//! migrating a call site, adding one, or writing a new test against the
//! raw surface is a conscious table edit reviewed with the change,
//! never drift.
//!
//! A count is a change DETECTOR, never the fence itself. It cannot tell
//! a lawful call from an unlawful substitution at the same count, so the
//! real fence in every case below is VISIBILITY: the raw operation is
//! private to its authority, and a consumer outside it does not compile.
//! The table names the residue that remains inside each authority, and
//! [`DELETED_ROADS`] names the doors that are bricked up rather than
//! merely unused.
//!
//! The needles are assembled at runtime so this file cannot match
//! itself; its comments deliberately avoid spelling them.

use std::path::{Path, PathBuf};

/// (file under `src/`, raw mints, hint constructors, progenitor
/// equality). Definitions and call sites alike — that is what makes the
/// count mechanical.
const INVENTORY: &[(&str, usize, usize, usize)] = &[
    ("names/birth.rs", 2, 26, 0),
    ("names/registry.rs", 3, 4, 0),
    ("names/tests.rs", 17, 18, 0),
];

/// Spellings that must not occur ANYWHERE under `src/`, with the reason a
/// reader needs to know why the door is bricked up rather than merely
/// unused.
///
/// This is the shape a transition ratchet ends in. A count that reaches
/// zero can be walked back by the next contributor who needs the shortcut;
/// a spelling that does not exist cannot. Each row is a road that was
/// migrated to its authority and then DELETED — reintroducing the name
/// fails here before it can acquire a second caller.
/// Each needle is assembled from two halves so this file cannot match
/// itself.
const DELETED_ROADS: &[(&str, &str, &str)] = &[
    (
        "born_",
        "scope(",
        "the raw-identity road out of a scope birth. A birth's one public \
         product is an admitted, live scope. A road that \
         converted a mint straight to a handle left the live environment \
         with no record, so nothing downstream could be required to \
         revalidate.",
    ),
    (
        "Scope",
        "Birth",
        "the deleted caller-facing relation-birth carrier. Exact semantic \
         forms choose their lexical scope inside the relation authority; SQL \
         lowering can allocate physical scopes but cannot state a semantic \
         birth policy.",
    ),
    (
        "republish_",
        "column(",
        "the deleted broad publication road. Semantic carrying belongs to \
         the exhaustive relation authority, while SQL aliases are physical \
         identities only.",
    ),
    (
        "republish_",
        "heading(",
        "the deleted broad whole-heading publication road.",
    ),
    (
        "Republish",
        "::",
        "the deleted caller-selected publication policy.",
    ),
    (
        "Heading",
        "Knowledge",
        "the deleted scope-heading sidecar. Semantic opacity and ordered ports live on the atomic relation interface.",
    ),
    (
        "known_",
        "heading(",
        "the deleted semantic heading recovery API.",
    ),
    (
        "mark_heading_",
        "opaque(",
        "the deleted post-construction opacity mutation.",
    ),
];

fn count(haystack: &str, needle: &str) -> usize {
    haystack.match_indices(needle).count()
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("source tree is readable") {
        let path = entry.expect("directory entry is readable").path();
        if path.is_dir() {
            walk(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn inventory_fence() {
    let mint = String::from("mint_") + "scope(";
    let mint_spellings = [
        mint.clone(),
        String::from("mint_derived_") + "scope(",
        String::from("mint_opaque_") + "scope(",
        String::from("mint_interior_") + "scope(",
    ];
    let hint = String::from("Hint") + "::";
    let progenitor = String::from("same_") + "value";

    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    walk(&src, &mut files);
    files.sort();

    let mut measured: Vec<(String, usize, usize, usize)> = Vec::new();
    for path in files {
        let text = std::fs::read_to_string(&path).expect("source file is readable");
        let mints: usize = mint_spellings.iter().map(|s| count(&text, s)).sum();
        let hints = count(&text, &hint);
        let sv = count(&text, &progenitor);
        if mints + hints + sv > 0 {
            let rel = path
                .strip_prefix(&src)
                .expect("walked path is under src")
                .to_string_lossy()
                .replace('\\', "/");
            measured.push((rel, mints, hints, sv));
        }
    }

    let mut drift = Vec::new();
    for (file, mints, hints, sv) in &measured {
        match INVENTORY.iter().find(|(f, ..)| f == file) {
            None => drift.push(format!(
                "UNLISTED {file}: mints={mints} hints={hints} progenitor={sv}"
            )),
            Some((_, m, h, s)) if (m, h, s) != (mints, hints, sv) => drift.push(format!(
                "MOVED {file}: mints {m}->{mints}, hints {h}->{hints}, progenitor {s}->{sv}"
            )),
            Some(_) => {}
        }
    }
    for (file, ..) in INVENTORY {
        if !measured.iter().any(|(f, ..)| f == file) {
            drift.push(format!("GONE {file}: listed but no longer matches"));
        }
    }
    assert!(
        drift.is_empty(),
        "the identity-surface inventory moved; reclassify the site and edit \
         names/fences.rs consciously with the change:\n  {}",
        drift.join("\n  ")
    );
}

#[test]
fn deleted_roads_stay_deleted() {
    // Assembled at runtime for the same reason the inventory needles are:
    // this file must not match itself.
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    walk(&src, &mut files);
    files.sort();
    let fence = src.join("names").join("fences.rs");

    let mut resurrected = Vec::new();
    for (head, tail, why) in DELETED_ROADS {
        let needle = String::from(*head) + tail;
        for path in &files {
            if path == &fence {
                continue;
            }
            let text = std::fs::read_to_string(path).expect("source file is readable");
            let hits = count(&text, &needle);
            if hits > 0 {
                let rel = path
                    .strip_prefix(&src)
                    .expect("walked path is under src")
                    .to_string_lossy()
                    .replace('\\', "/");
                resurrected.push(format!("{rel}: {hits} occurrence(s) — {why}"));
            }
        }
    }
    assert!(
        resurrected.is_empty(),
        "a deleted identity road was written again:\n  {}",
        resurrected.join("\n  ")
    );
}
