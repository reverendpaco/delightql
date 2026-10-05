// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Csvstruct command handler
//!
//! Handles: dql tools csvstruct '<query>'
//! Reads CSV from stdin into table c(...), then runs the user's DQL query.
//! With --has-headers, the first row names the columns (stropped).
//! Without, columns are named c1, c2, c3, ... The staging — and the value
//! each field acquires — is `csv_table`'s.

use crate::args;
use crate::connection;
use crate::output_format::OutputFormat;
use anyhow::Result;
use rusqlite::Connection;
use std::io::{self, IsTerminal, Read};

pub fn handle_csvstruct_command(
    query: &str,
    format: Option<OutputFormat>,
    to: Option<args::Stage>,
    has_headers: bool,
    delimiter: &str,
    _base_args: &args::CliArgs,
) -> Result<()> {
    if io::stdin().is_terminal() {
        anyhow::bail!(
            "dql tools csvstruct requires CSV piped to stdin.\n\
             Usage: cat data.csv | dql tools csvstruct '<query>'"
        );
    }

    let delim_byte = match delimiter {
        "\\t" | "tab" => b'\t',
        s if s.len() == 1 => s.as_bytes()[0],
        _ => anyhow::bail!("Delimiter must be a single character (or \\t / tab)"),
    };

    let mut raw = Vec::new();
    io::stdin().read_to_end(&mut raw)?;
    if raw.is_empty() {
        anyhow::bail!("No CSV input provided");
    }

    // A pid alone is not unique: sandboxed/containerized runs give
    // concurrent processes the same pid in their own namespaces, and
    // the shared temp dir then collides deterministically. tempfile
    // creates the name atomically (O_EXCL) under the OS.
    let temp_file = tempfile::Builder::new()
        .prefix("dql_csvstruct_")
        .suffix(".db")
        .tempfile()?;
    let temp_path = temp_file.path().to_path_buf();
    {
        let conn = Connection::open(&temp_path)?;
        super::csv_table::stage_csv_table(&conn, "c", has_headers, delim_byte, &raw, "stdin")?;
    }

    let db_path_str = temp_path.to_string_lossy().to_string();
    let output_format = format.unwrap_or(OutputFormat::Table);

    let mut handle = connection::open_handle(connection::SessionProfile::client())?;
    {
        let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
        // A higher-order directive writes both groups: `(arguments)(receipt
        // access)`. A lone group is receipt access by position, so dropping
        // the `(*)` binds zero arguments and the demand refuses on arity.
        crate::exec_ng::run_dql_query(
            &format!("mount!(\"{}\", \"main\")(*)", db_path_str),
            &mut *session,
        )?;
    }

    let result = crate::exec_ng::execute_query(
        query,
        &mut *handle,
        to,
        crate::exec_ng::Rendering {
            format: output_format,
            no_headers: false,
            no_sanitize: false,
        },
        crate::exec_ng::ShippedSets::Discarded,
        false,
    );

    drop(handle);
    let _ = std::fs::remove_file(&temp_path);

    result.map(|_| ())
}
