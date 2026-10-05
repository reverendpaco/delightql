// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Filemunge command handler
//!
//! Handles: dql tools filemunge --table name:format[:noheader] path ... '<query>'
//! Loads multiple tables from files (csv, tsv, json-singleton) into a temp DB,
//! then runs the user's DQL query against all of them.

use crate::args;
use crate::connection;
use crate::output_format::OutputFormat;
use anyhow::Result;
use rusqlite::Connection;
use std::io::Read;
use std::path::Path;

fn strop(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

struct TableSpec {
    name: String,
    format: TableFormat,
    has_headers: bool,
    path: String,
}

enum TableFormat {
    Csv,
    Tsv,
    JsonSingleton,
}

fn parse_table_spec(spec_part: &str, path: &str) -> Result<TableSpec> {
    let parts: Vec<&str> = spec_part.split(':').collect();
    if parts.len() < 2 || parts.len() > 3 {
        anyhow::bail!(
            "Invalid table spec '{}': expected name:format or name:format:noheader",
            spec_part
        );
    }

    let name = parts[0].to_string();
    if name.is_empty() {
        anyhow::bail!("Table name cannot be empty in spec '{}'", spec_part);
    }

    let (format, default_headers) = match parts[1] {
        "csv" => (TableFormat::Csv, true),
        "tsv" => (TableFormat::Tsv, true),
        "json-singleton" => (TableFormat::JsonSingleton, false),
        other => anyhow::bail!(
            "Unknown format '{}'. Expected: csv, tsv, json-singleton",
            other
        ),
    };

    let has_headers = if parts.len() == 3 {
        match parts[2] {
            "header" => true,
            "noheader" => false,
            other => anyhow::bail!(
                "Unknown header mode '{}'. Expected: header or noheader",
                other
            ),
        }
    } else {
        default_headers
    };

    Ok(TableSpec {
        name,
        format,
        has_headers,
        path: path.to_string(),
    })
}

fn load_csv_table(conn: &Connection, spec: &TableSpec, delimiter: u8) -> Result<()> {
    let data = read_file(&spec.path)?;
    super::csv_table::stage_csv_table(
        conn,
        &spec.name,
        spec.has_headers,
        delimiter,
        &data,
        &format!("'{}'", spec.path),
    )
}

fn load_json_singleton_table(conn: &Connection, spec: &TableSpec) -> Result<()> {
    let data = read_file(&spec.path)?;
    let text = String::from_utf8(data)
        .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in '{}': {}", spec.path, e))?;

    if text.trim().is_empty() {
        anyhow::bail!("Empty JSON file '{}'", spec.path);
    }

    let table_name = strop(&spec.name);
    conn.execute(&format!("CREATE TABLE {} (j TEXT)", table_name), [])?;
    conn.execute(
        &format!("INSERT INTO {} (j) VALUES (?1)", table_name),
        [&text],
    )?;

    Ok(())
}

fn read_file(path: &str) -> Result<Vec<u8>> {
    let p = Path::new(path);
    // Support process substitution (/dev/fd/N) and regular files
    let mut f =
        std::fs::File::open(p).map_err(|e| anyhow::anyhow!("Cannot open '{}': {}", path, e))?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(buf)
}

pub fn handle_filemunge_command(
    query: &str,
    tables: &[String],
    format: Option<OutputFormat>,
    to: Option<args::Stage>,
    _base_args: &args::CliArgs,
) -> Result<()> {
    if tables.is_empty() {
        anyhow::bail!("No --table specs provided");
    }

    if tables.len() % 2 != 0 {
        anyhow::bail!("Each --table requires a SPEC and a PATH");
    }

    let specs: Vec<TableSpec> = tables
        .chunks(2)
        .map(|pair| parse_table_spec(&pair[0], &pair[1]))
        .collect::<Result<_>>()?;

    // A pid alone is not unique: sandboxed/containerized runs give
    // concurrent processes the same pid in their own namespaces, and
    // the shared temp dir then collides deterministically. tempfile
    // creates the name atomically (O_EXCL) under the OS.
    let temp_file = tempfile::Builder::new()
        .prefix("dql_filemunge_")
        .suffix(".db")
        .tempfile()?;
    let temp_path = temp_file.path().to_path_buf();

    {
        let conn = Connection::open(&temp_path)?;
        for spec in &specs {
            match spec.format {
                TableFormat::Csv => load_csv_table(&conn, spec, b',')?,
                TableFormat::Tsv => load_csv_table(&conn, spec, b'\t')?,
                TableFormat::JsonSingleton => load_json_singleton_table(&conn, spec)?,
            }
        }
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
