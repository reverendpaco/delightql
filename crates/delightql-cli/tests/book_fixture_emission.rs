// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The emitter's invariants, stated as a FIXTURE BOOK and compared
//! golden-file style through the real pipeline: bundler -> compliant
//! sqlite -> `dql book --db` -> emission == expected.md. The ball-corpus
//! philosophy applied to the book machinery (tests-are-invariants):
//! nothing here touches a struct, a function, or authored content — the
//! fixture atoms in tests/fixtures/book/ ARE the invariant statements
//! (per-depth shifting, merged attribute blocks, unmarked pass-through,
//! indented-heading hash placement, indented-code and nested-fence
//! immunity, preorder return from a subtree), and expected.md is their
//! single source of expected truth. Rename an internal function and
//! nothing fires; break a promise and the diff names it.
//!
//! Why this is a Rust test and not a corpus ball: the corpus harness
//! speaks DQL-to-results; this pins the `dql book` subcommand's
//! emission, a process-level contract of the cli binary.

use std::path::Path;
use std::process::Command;

#[test]
fn fixture_book_emits_the_golden_file() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let fixtures = manifest.join("tests/fixtures/book");
    let bundler = manifest.join("../../assets/bin/bundle.py");
    let db = std::env::temp_dir().join(format!("dql-fixture-book-{}.sqlite", std::process::id()));
    let _ = std::fs::remove_file(&db);

    // Stage 1: the real bundler, on the fixture pool (needs uv, same as
    // building the workspace does).
    let bundled = Command::new("uv")
        .arg("run")
        .arg(&bundler)
        .arg("books")
        .arg("--content")
        .arg(&fixtures)
        .arg("--output")
        .arg(&db)
        .arg("--content-version")
        .arg("fixture")
        .output()
        .expect("run the bundler under uv");
    assert!(
        bundled.status.success(),
        "bundler refused the fixture pool: {}",
        String::from_utf8_lossy(&bundled.stderr)
    );

    // Stage 2: the real binary, the real --db door.
    let emitted = Command::new(env!("CARGO_BIN_EXE_dql"))
        .args(["book", "fixture", "--db"])
        .arg(&db)
        .output()
        .expect("run dql book");
    assert!(
        emitted.status.success(),
        "dql book refused the fixture bundle: {}",
        String::from_utf8_lossy(&emitted.stderr)
    );
    let _ = std::fs::remove_file(&db);

    // Stage 3: golden comparison. On mismatch, write the actual emission
    // beside the expected file so a human (or agent) can diff and — if
    // the change was intended — re-bless by overwriting expected.md.
    let expected = std::fs::read_to_string(fixtures.join("expected.md")).unwrap();
    let actual = String::from_utf8_lossy(&emitted.stdout).to_string();
    if actual != expected {
        let got = fixtures.join("expected.md.got");
        std::fs::write(&got, &actual).unwrap();
        panic!(
            "fixture emission diverged from the golden file.\n  expected: {}\n  actual:   {}\n  diff them; overwrite expected.md to bless an intended change",
            fixtures.join("expected.md").display(),
            got.display()
        );
    }
}
