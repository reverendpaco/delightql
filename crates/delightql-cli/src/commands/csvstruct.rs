//! Csvstruct command handler
//!
//! Handles: dql tools csvstruct '<query>'
//! Reads CSV from stdin into table c(...), then runs the user's DQL query.
//! With --has-headers, the first row names the columns (stropped).
//! Without, columns are named c1, c2, c3, ...

use crate::args;
use crate::connection;
use crate::output_format::OutputFormat;
use anyhow::Result;
use rusqlite::Connection;
use std::io::{self, IsTerminal, Read};

fn strop(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

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

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(has_headers)
        .delimiter(delim_byte)
        .from_reader(raw.as_slice());

    // Determine column names
    let col_names: Vec<String> = if has_headers {
        rdr.headers()?.iter().map(|h| h.to_string()).collect()
    } else {
        // Peek first record to get width
        let width = match rdr.records().next() {
            Some(Ok(ref rec)) => rec.len(),
            Some(Err(e)) => return Err(e.into()),
            None => anyhow::bail!("No CSV records found"),
        };
        (1..=width).map(|i| format!("c{}", i)).collect()
    };

    if col_names.is_empty() {
        anyhow::bail!("CSV has zero columns");
    }

    // Build CREATE TABLE with stropped column names, no type affinity
    let col_defs: Vec<String> = col_names.iter().map(|n| strop(n)).collect();
    let create_sql = format!("CREATE TABLE c ({})", col_defs.join(", "));

    let placeholders: Vec<String> = (1..=col_names.len()).map(|i| format!("?{}", i)).collect();
    let insert_sql = format!("INSERT INTO c VALUES ({})", placeholders.join(", "));

    let temp_path = std::env::temp_dir().join(format!("dql_csvstruct_{}.db", std::process::id()));
    {
        let conn = Connection::open(&temp_path)?;
        conn.execute(&create_sql, [])?;

        // For no-headers mode we need to re-read from the start since we consumed
        // the first record during width detection.
        if !has_headers {
            drop(rdr);
            let mut rdr2 = csv::ReaderBuilder::new()
                .has_headers(false)
                .delimiter(delim_byte)
                .from_reader(raw.as_slice());
            insert_records(&conn, &insert_sql, col_names.len(), rdr2.records())?;
        } else {
            insert_records(&conn, &insert_sql, col_names.len(), rdr.records())?;
        }
    }

    let db_path_str = temp_path.to_string_lossy().to_string();
    let conn = connection::ConnectionManager::new_memory()?;
    let output_format = format.unwrap_or(OutputFormat::Table);

    let mut handle = conn.open_handle()?;
    let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;

    crate::exec_ng::run_dql_query(
        &format!("mount!(\"{}\", \"main\")", db_path_str),
        &mut *session,
    )?;

    let result =
        crate::exec_ng::execute_query(query, &mut *session, to, output_format, false, false, false);

    drop(session);
    drop(handle);
    drop(conn);
    let _ = std::fs::remove_file(&temp_path);

    result.map(|_| ())
}

fn insert_records(
    conn: &Connection,
    insert_sql: &str,
    ncols: usize,
    records: csv::StringRecordsIter<&[u8]>,
) -> Result<()> {
    let mut stmt = conn.prepare(insert_sql)?;
    for rec in records {
        let rec = rec?;
        let params: Vec<Option<&str>> = (0..ncols).map(|i| rec.get(i)).collect();
        stmt.execute(rusqlite::params_from_iter(params.iter()))?;
    }
    Ok(())
}
