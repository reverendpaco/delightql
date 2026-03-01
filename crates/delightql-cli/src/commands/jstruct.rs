//! Jstruct command handler
//!
//! Handles: dql tools jstruct '<query>'
//! Reads JSON from stdin into table j(j TEXT), then runs the user's DQL query.

use crate::args;
use crate::connection;
use crate::output_format::OutputFormat;
use anyhow::Result;
use rusqlite::Connection;
use std::io::{self, IsTerminal, Read};

/// Handle jstruct command
pub fn handle_jstruct_command(
    query: &str,
    format: Option<OutputFormat>,
    to: Option<args::Stage>,
    _base_args: &args::CliArgs,
) -> Result<()> {
    if io::stdin().is_terminal() {
        anyhow::bail!(
            "dql tools jstruct requires JSON piped to stdin.\nUsage: cat data.json | dql tools jstruct '<query>'"
        );
    }

    let mut buffer = String::new();
    io::stdin().read_to_string(&mut buffer)?;

    if buffer.trim().is_empty() {
        anyhow::bail!("No JSON input provided");
    }

    // Create temp db with table j(j TEXT)
    let temp_path = std::env::temp_dir().join(format!("dql_jstruct_{}.db", std::process::id()));
    {
        let conn = Connection::open(&temp_path)?;
        conn.execute("CREATE TABLE j (j TEXT)", [])?;
        conn.execute("INSERT INTO j (j) VALUES (?1)", [&buffer])?;
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

    let result = crate::exec_ng::execute_query(
        query,
        &mut *session,
        to,
        output_format,
        false,
        false,
        false,
    );

    // Drop SQLite connections before unlinking the temp file
    drop(session);
    drop(handle);
    drop(conn);
    let _ = std::fs::remove_file(&temp_path);

    result.map(|_| ())
}
