// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `dql editor export-artifacts` — write the embedded editor-support
//! bundle to disk, serving its two consumers with one layout:
//!
//! ```text
//! <dir>/parser/delightql.so        editor runtimepath: the compiled parser
//! <dir>/queries/delightql/*.scm    editor runtimepath: highlight queries
//! <dir>/grammar/…                  rebuildable tree-sitter project
//! ```
//!
//! Point an editor's runtimepath at `<dir>` and it finds parser and
//! queries by convention; `cd <dir>/grammar` is a complete tree-sitter
//! project for rebuilding on another platform. The embedded parser only
//! fits the platform this binary was built for — the closing line names
//! that target and ABI so a mismatch is visible before dlopen.

use anyhow::Result;
use std::path::Path;

const NAMESPACE: &str = "cli::editor";

/// The bundle stores basenames (the bundler collects a flat directory);
/// re-check at the write seam so a noncompliant image cannot steer a
/// path outside the export directory.
fn plain_basename(name: &str) -> Result<&str> {
    anyhow::ensure!(
        !name.is_empty() && !name.contains(['/', '\\']) && name != "." && name != "..",
        "bundle entry '{name}' is not a plain file name"
    );
    Ok(name)
}

fn write_file(dir: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let path = dir.join(plain_basename(name)?);
    std::fs::write(&path, bytes)?;
    println!("{}", path.display());
    Ok(())
}

pub fn handle_export_artifacts(dir: &Path) -> Result<()> {
    let mut handle = crate::connection::open_handle()?;
    let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
    // A higher-order directive writes both groups: `(arguments)(receipt
    // access)`. A lone group is receipt access by position, so dropping the
    // `(*)` binds zero arguments and the demand refuses on arity. Receipt
    // rows are discarded.
    crate::exec_ng::fetch_all(
        &mut *session,
        &format!("mount!(\"delightql-bytes://editor\", \"{NAMESPACE}\")(*)"),
    )?;
    crate::embedded_db::verify_bundle_schema_version(&mut *session, NAMESPACE)?;

    let grammar_dir = dir.join("grammar");
    let queries_dir = dir.join("queries").join("delightql");
    let parser_dir = dir.join("parser");
    for d in [&grammar_dir, &queries_dir, &parser_dir] {
        std::fs::create_dir_all(d)?;
    }

    let sources = crate::exec_ng::fetch_all(
        &mut *session,
        &format!("{NAMESPACE}.grammar_source(*) |> #(path) |> (path, content)"),
    )?;
    for row in &sources.rows {
        write_file(&grammar_dir, &row[0], row[1].as_bytes())?;
    }

    let queries = crate::exec_ng::fetch_all(
        &mut *session,
        &format!("{NAMESPACE}.editor_query(*) |> #(name) |> (name, content)"),
    )?;
    for row in &queries.rows {
        write_file(&queries_dir, &format!("{}.scm", row[0]), row[1].as_bytes())?;
    }

    let meta = crate::exec_ng::fetch_all(
        &mut *session,
        &format!("{NAMESPACE}.parser_artifact(*) |> (target, abi_version)"),
    )?;
    anyhow::ensure!(
        meta.rows.len() == 1,
        "embedded bundle holds {} parser artifacts, expected exactly one",
        meta.rows.len()
    );
    // The blob rides the raw protocol channel — a display rendering is
    // not byte-faithful.
    let (_cols, blob_rows) = crate::exec_ng::fetch_all_raw(
        &mut *session,
        &format!("{NAMESPACE}.parser_artifact(*) |> (content)"),
    )?;
    let so_bytes = blob_rows
        .first()
        .and_then(|row| row.first())
        .and_then(|cell| cell.as_deref())
        .ok_or_else(|| anyhow::anyhow!("parser artifact content is absent"))?;
    write_file(&parser_dir, "delightql.so", so_bytes)?;

    println!(
        "exported parser for {} (tree-sitter ABI {})",
        meta.rows[0][0], meta.rows[0][1]
    );
    Ok(())
}
