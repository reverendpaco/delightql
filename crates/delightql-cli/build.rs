// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use rusqlite::Connection;

const BOOK_APPLICATION_ID: i64 = 0x4451_4c42; // DQLB
const MAN_APPLICATION_ID: i64 = 0x4451_4c4d; // DQLM
const EDITOR_APPLICATION_ID: i64 = 0x4451_4c45; // DQLE
const DOC_SCHEMA_VERSION: i64 = 1;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let repo_root = manifest_dir
        .join("../..")
        .canonicalize()
        .expect("repository root");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));

    let target = env::var("TARGET").expect("TARGET");
    run_asset_front_door(&repo_root, &target);
    build_book_database(&repo_root, &out_dir.join("book.sqlite"));
    build_man_database(&repo_root, &out_dir.join("man.sqlite"));
    build_editor_database(&repo_root, &out_dir.join("editor.sqlite"), &target);

    // Capture version from Cargo.toml
    let version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=BUILD_VERSION={}", version);

    // Capture jj change ID (first 8 chars for brief display)
    // Commented out to speed up builds - jj operations are slow with large history
    /*
    let change_id = Command::new("jj")
        .args(["log", "-r", "@", "-T", "change_id.short()"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unavailable".to_string());

    let change_id_full = Command::new("jj")
        .args(["log", "-r", "@", "-T", "change_id"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unavailable".to_string());

    // Capture jj commit ID
    let commit_id = Command::new("jj")
        .args(["log", "-r", "@", "-T", "commit_id"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unavailable".to_string());

    // Capture jj description
    let description = Command::new("jj")
        .args(["log", "-r", "@", "-T", "description"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout)
                    .ok()
                    .map(|s| s.lines().next().unwrap_or("").to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unavailable".to_string());
    */

    let change_id = "unavailable".to_string();
    let change_id_full = "unavailable".to_string();
    let commit_id = "unavailable".to_string();
    let description = "unavailable".to_string();

    // Capture build timestamp
    let build_time = chrono::Utc::now().to_rfc3339();

    // Capture build date (for brief display)
    let build_date = chrono::Utc::now().format("%Y-%m-%d").to_string();

    // Set environment variables for use in the code
    println!("cargo:rustc-env=BUILD_CHANGE_ID_SHORT={}", change_id.trim());
    println!("cargo:rustc-env=BUILD_CHANGE_ID={}", change_id_full.trim());
    println!("cargo:rustc-env=BUILD_COMMIT_ID={}", commit_id.trim());
    println!("cargo:rustc-env=BUILD_DESCRIPTION={}", description.trim());
    println!("cargo:rustc-env=BUILD_TIME={}", build_time);
    println!("cargo:rustc-env=BUILD_DATE={}", build_date);

    // Target triple
    let target = env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=BUILD_TARGET={}", target);

    // Profile (debug/release)
    let profile = env::var("PROFILE").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=BUILD_PROFILE={}", profile);

    // Capture rustc version
    let rustc_version = Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=BUILD_RUSTC_VERSION={}", rustc_version);

    // Rebuild if .jj directory changes
    // Commented out to avoid watching large .jj directory
    // println!("cargo:rerun-if-changed=../../.jj");
    println!("cargo:rerun-if-changed=build.rs");
}

/// The assets front door: one make invocation
/// produces every embedded database; build.rs synthesizes nothing. It
/// knows two things — how to ask for the files (this function) and how
/// to refuse them (the verifications below). *Assume existence, verify
/// compliance* — the bundler is trusted for nothing.
fn run_asset_front_door(repo_root: &Path, target: &str) {
    let assets_dir = repo_root.join("assets");
    // TARGET pins the platform recorded in the editor bundle's
    // parser_artifact; build_editor_database refuses a mismatch, so a
    // cross-compile without a target-capable cc fails here, loudly.
    let status = Command::new("make")
        .arg("-C")
        .arg(&assets_dir)
        .arg(format!("TARGET={target}"))
        .status()
        .expect("run the assets front door (needs make + uv + tree-sitter + cc; the bundler declares its own python deps)");
    assert!(
        status.success(),
        "asset bundling failed: make -C {}",
        assets_dir.display()
    );
    // Watch the inputs, not the products: rerun-if-changed on bundled/
    // (or on grammar/src, grammar/delightql.so) would re-trigger cargo
    // after every make run.
    for input in ["books", "man", "bin/bundle.py", "Makefile"] {
        println!(
            "cargo:rerun-if-changed={}",
            assets_dir.join(input).display()
        );
    }
    for input in [
        "grammar/grammar.js",
        "grammar/tokens.js",
        "grammar/conflicts.js",
        "grammar/package.json",
        "grammar/tree-sitter.json",
        "grammar/queries",
    ] {
        println!(
            "cargo:rerun-if-changed={}",
            repo_root.join(input).display()
        );
    }
}

/// The book bundle's side of the seam: verify the contract, embed.
fn build_book_database(repo_root: &Path, output: &Path) {
    let bundled = repo_root.join("assets/bundled/books.sqlite");
    let conn = Connection::open(&bundled)
        .unwrap_or_else(|e| panic!("open bundled book {}: {e}", bundled.display()));
    validate_database(
        &conn,
        BOOK_APPLICATION_ID,
        &["base_content", "book", "book_meta", "bundle_meta", "image"],
    );
    // Shapes, not spellings: table_info catches a same-named table with
    // missing/renamed/retyped columns or a different PK, which name-list
    // validation waves through. (name, declared type, pk position).
    expect_table_shape(
        &conn,
        "bundle_meta",
        &[
            ("singleton", "INTEGER", 1),
            ("schema_version", "INTEGER", 0),
            ("content_version", "TEXT", 0),
            ("source_digest", "TEXT", 0),
        ],
    );
    expect_table_shape(
        &conn,
        "base_content",
        &[
            ("slug", "TEXT", 1),
            ("title", "TEXT", 0),
            ("content", "TEXT", 0),
            ("source_path", "TEXT", 0),
            ("content_digest", "TEXT", 0),
        ],
    );
    expect_table_shape(
        &conn,
        "book",
        &[
            ("book_name", "TEXT", 1),
            ("ordinal", "INTEGER", 2),
            ("heading_shift", "INTEGER", 0),
            ("slug", "TEXT", 0),
        ],
    );
    expect_table_shape(
        &conn,
        "book_meta",
        &[
            ("book_name", "TEXT", 1),
            ("title", "TEXT", 0),
            ("frontmatter", "TEXT", 0),
        ],
    );
    expect_table_shape(
        &conn,
        "image",
        &[
            ("name", "TEXT", 1),
            ("media_type", "TEXT", 0),
            ("content", "BLOB", 0),
            ("content_digest", "TEXT", 0),
        ],
    );
    let fk: (String, String, String) = conn
        .query_row("PRAGMA foreign_key_list(book)", [], |row| {
            Ok((row.get(2)?, row.get(3)?, row.get(4)?))
        })
        .expect("book declares its foreign key");
    assert_eq!(
        fk,
        (
            "base_content".to_string(),
            "slug".to_string(),
            "slug".to_string()
        ),
        "book -> base_content foreign key edge"
    );
    let fk_violations: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_foreign_key_check()",
            [],
            |row| row.get(0),
        )
        .expect("run foreign_key_check");
    assert_eq!(fk_violations, 0, "foreign key violations in bundled book");

    // Data properties directly, rather than trusting the producer's CHECK
    // constraints to exist: query_row would accept the FIRST of many
    // bundle_meta rows, so count them.
    let meta_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM bundle_meta", [], |row| row.get(0))
        .expect("count bundle_meta rows");
    assert_eq!(meta_rows, 1, "bundle_meta must hold exactly one row");
    let (singleton, schema_version): (i64, i64) = conn
        .query_row(
            "SELECT singleton, schema_version FROM bundle_meta",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read bundle_meta");
    assert_eq!(singleton, 1, "bundle_meta singleton");
    assert_eq!(
        schema_version, DOC_SCHEMA_VERSION,
        "bundle_meta schema_version"
    );
    let bad_placements: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM book WHERE ordinal < 1 OR heading_shift < 0",
            [],
            |row| row.get(0),
        )
        .expect("check placement invariants");
    assert_eq!(bad_placements, 0, "book rows violate ordinal/shift bounds");
    drop(conn);

    let temp = output.with_extension("sqlite.tmp");
    let _ = fs::remove_file(&temp);
    fs::copy(&bundled, &temp).unwrap_or_else(|e| panic!("stage bundled book into OUT_DIR: {e}"));
    publish(&temp, output);

}

/// The man bundle's side of the seam: same ask-and-refuse shape as the
/// book. The bundler (assets/bin/bundle.py man) produced the file; this
/// verifies the DQLM contract and embeds it.
fn build_man_database(repo_root: &Path, output: &Path) {
    let bundled = repo_root.join("assets/bundled/man.sqlite");
    let conn = Connection::open(&bundled)
        .unwrap_or_else(|e| panic!("open bundled man {}: {e}", bundled.display()));
    validate_database(&conn, MAN_APPLICATION_ID, &["bundle_meta", "man_page"]);
    expect_table_shape(
        &conn,
        "bundle_meta",
        &[
            ("singleton", "INTEGER", 1),
            ("schema_version", "INTEGER", 0),
            ("content_version", "TEXT", 0),
            ("source_digest", "TEXT", 0),
        ],
    );
    expect_table_shape(
        &conn,
        "man_page",
        &[
            ("name", "TEXT", 1),
            ("section", "INTEGER", 2),
            ("troff", "TEXT", 0),
            ("content_digest", "TEXT", 0),
        ],
    );
    let meta_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM bundle_meta", [], |row| row.get(0))
        .expect("count man bundle_meta rows");
    assert_eq!(meta_rows, 1, "man bundle_meta must hold exactly one row");
    let (singleton, schema_version): (i64, i64) = conn
        .query_row(
            "SELECT singleton, schema_version FROM bundle_meta",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read man bundle_meta");
    assert_eq!(singleton, 1, "man bundle_meta singleton");
    assert_eq!(
        schema_version, DOC_SCHEMA_VERSION,
        "man bundle_meta schema_version"
    );
    let pages: i64 = conn
        .query_row("SELECT COUNT(*) FROM man_page", [], |row| row.get(0))
        .expect("count man pages");
    assert!(pages > 0, "man bundle contains no pages");
    drop(conn);

    let temp = output.with_extension("sqlite.tmp");
    let _ = fs::remove_file(&temp);
    fs::copy(&bundled, &temp).unwrap_or_else(|e| panic!("stage bundled man into OUT_DIR: {e}"));
    publish(&temp, output);
}

/// The editor bundle's side of the seam: same ask-and-refuse shape as
/// book and man, plus the platform check no other bundle needs — the
/// compiled parser is target-specific, so the artifact's recorded triple
/// must be the one this binary is being built for.
fn build_editor_database(repo_root: &Path, output: &Path, target: &str) {
    let bundled = repo_root.join("assets/bundled/editor.sqlite");
    let conn = Connection::open(&bundled)
        .unwrap_or_else(|e| panic!("open bundled editor {}: {e}", bundled.display()));
    validate_database(
        &conn,
        EDITOR_APPLICATION_ID,
        &["bundle_meta", "editor_query", "grammar_source", "parser_artifact"],
    );
    expect_table_shape(
        &conn,
        "bundle_meta",
        &[
            ("singleton", "INTEGER", 1),
            ("schema_version", "INTEGER", 0),
            ("content_version", "TEXT", 0),
            ("source_digest", "TEXT", 0),
        ],
    );
    expect_table_shape(
        &conn,
        "grammar_source",
        &[
            ("path", "TEXT", 1),
            ("content", "TEXT", 0),
            ("content_digest", "TEXT", 0),
        ],
    );
    expect_table_shape(
        &conn,
        "editor_query",
        &[
            ("name", "TEXT", 1),
            ("content", "TEXT", 0),
            ("content_digest", "TEXT", 0),
        ],
    );
    expect_table_shape(
        &conn,
        "parser_artifact",
        &[
            ("target", "TEXT", 1),
            ("abi_version", "INTEGER", 0),
            ("content", "BLOB", 0),
            ("content_digest", "TEXT", 0),
        ],
    );
    let meta_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM bundle_meta", [], |row| row.get(0))
        .expect("count editor bundle_meta rows");
    assert_eq!(meta_rows, 1, "editor bundle_meta must hold exactly one row");
    let (singleton, schema_version): (i64, i64) = conn
        .query_row(
            "SELECT singleton, schema_version FROM bundle_meta",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read editor bundle_meta");
    assert_eq!(singleton, 1, "editor bundle_meta singleton");
    assert_eq!(
        schema_version, DOC_SCHEMA_VERSION,
        "editor bundle_meta schema_version"
    );
    let grammar_js: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM grammar_source WHERE path = 'grammar.js'",
            [],
            |row| row.get(0),
        )
        .expect("look for grammar.js");
    assert_eq!(grammar_js, 1, "editor bundle carries no grammar.js");
    let queries: i64 = conn
        .query_row("SELECT COUNT(*) FROM editor_query", [], |row| row.get(0))
        .expect("count editor queries");
    assert!(queries > 0, "editor bundle contains no queries");
    let artifacts: i64 = conn
        .query_row("SELECT COUNT(*) FROM parser_artifact", [], |row| row.get(0))
        .expect("count parser artifacts");
    assert_eq!(artifacts, 1, "exactly one parser artifact per binary");
    let (artifact_target, abi_version, magic): (String, i64, Vec<u8>) = conn
        .query_row(
            "SELECT target, abi_version, substr(content, 1, 4) FROM parser_artifact",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("read parser artifact");
    assert_eq!(
        artifact_target, target,
        "parser artifact was compiled for a different platform than this binary"
    );
    assert!(abi_version >= 1, "parser artifact abi_version");
    let is_shared_lib = magic.starts_with(b"\x7fELF")
        || magic.starts_with(&[0xcf, 0xfa, 0xed, 0xfe])
        || magic.starts_with(&[0xca, 0xfe, 0xba, 0xbe])
        || magic.starts_with(b"MZ");
    assert!(is_shared_lib, "parser artifact is not a shared library");
    drop(conn);

    let temp = output.with_extension("sqlite.tmp");
    let _ = fs::remove_file(&temp);
    fs::copy(&bundled, &temp)
        .unwrap_or_else(|e| panic!("stage bundled editor into OUT_DIR: {e}"));
    publish(&temp, output);
}

/// Assert a table's exact column shape: (name, declared type, position in
/// the primary key — 0 when not part of it). Catches a same-named table
/// with missing, renamed, or retyped columns, or a different key.
fn expect_table_shape(conn: &Connection, table: &str, expected: &[(&str, &str, i32)]) {
    let mut statement = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .expect("prepare table_info");
    let actual = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i32>(5)?,
            ))
        })
        .expect("query table_info")
        .map(|row| row.expect("table_info row"))
        .collect::<Vec<_>>();
    let expected = expected
        .iter()
        .map(|(name, ty, pk)| (name.to_string(), ty.to_string(), *pk))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{table} column shape");
}

fn validate_database(conn: &Connection, application_id: i64, expected_tables: &[&str]) {
    let actual_id: i64 = conn
        .query_row("PRAGMA application_id", [], |row| row.get(0))
        .expect("read application_id");
    assert_eq!(actual_id, application_id, "database application_id");
    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("read user_version");
    assert_eq!(version, DOC_SCHEMA_VERSION, "database user_version");
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("run integrity_check");
    assert_eq!(integrity, "ok", "generated database integrity");
    let mut statement = conn
        .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")
        .expect("prepare schema validation");
    let actual = statement
        .query_map([], |row| row.get::<_, String>(0))
        .expect("query schema")
        .map(|row| row.expect("schema row"))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected_tables, "generated database tables");
}

fn publish(temp: &Path, output: &Path) {
    let _ = fs::remove_file(output);
    fs::rename(temp, output).unwrap_or_else(|e| panic!("publish {}: {e}", output.display()));
}
