//! Query command handler — plumbing layer.
//!
//! Creates connections, handles, and sessions. Passes the session to exec_ng.
//! Like listener.rs is plumbing for handler.rs.

use crate::args::{self, CliArgs, Command};
use crate::output_format::OutputFormat;
use crate::{connection, exec_ng};
use anyhow::Result;
use std::io::{self, IsTerminal, Read};
use std::path::Path;

fn check_database_exists(db_path: &str, make_new_db_if_missing: bool) -> Result<()> {
    if db_path.starts_with("pipe://") {
        return Ok(());
    }
    if !make_new_db_if_missing && !Path::new(db_path).exists() {
        anyhow::bail!(
            "Database file '{}' does not exist. Use --make-new-db-if-missing to create it.",
            db_path
        );
    }
    Ok(())
}

fn make_connection(
    db_path: &Option<String>,
    make_new_db_if_missing: bool,
) -> Result<connection::ConnectionManager> {
    if let Some(ref path) = db_path {
        check_database_exists(path, make_new_db_if_missing)?;
        connection::ConnectionManager::new_file(path)
    } else {
        connection::ConnectionManager::new_memory()
    }
}

/// Execute a query string: create session via handle, call exec_ng.
fn run_query(
    source: &str,
    handle: &mut dyn delightql_core::api::DqlHandle,
    to: Option<args::Stage>,
    output_format: OutputFormat,
    no_headers: bool,
    no_sanitize: bool,
    sequential: bool,
) -> Result<()> {
    let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
    exec_ng::execute_query(
        source,
        &mut *session,
        to,
        output_format,
        no_headers,
        no_sanitize,
        sequential,
    )?;
    Ok(())
}

/// Handle query subcommand
pub fn handle_query_subcommand(command: &Command, base_args: &CliArgs) -> Result<()> {
    let Command::Query {
        query,
        file,
        to,
        format,
        no_headers,
        no_sanitize,
        make_new_db_if_missing,
        consult_files,
        attach,
        sql_optimize,
        inline_ctes,
        sequential,
        #[cfg(feature = "repl")]
            interactive: _,
        #[cfg(feature = "repl")]
        quiet,
        #[cfg(feature = "repl")]
        highlights,
        ..
    } = command
    else {
        unreachable!("handle_query_subcommand called with non-Query command")
    };

    let output_format = OutputFormat::resolve(format.clone());
    let db_path = base_args
        .database
        .as_ref()
        .map(|p| p.to_string_lossy().to_string());

    if *sql_optimize > 0 {
        eprintln!("warning: --soptimize currently does nothing");
    }
    if *inline_ctes {
        eprintln!("warning: --inline-ctes currently does nothing");
    }

    if *no_sanitize {
        eprintln!("warning: output sanitization disabled, terminal injection possible");
    }

    if !consult_files.is_empty() {
        anyhow::bail!("--consult flag not supported. Use consult!() in DQL source instead.");
    }

    let conn = make_connection(&db_path, *make_new_db_if_missing)?;
    let mut handle = conn.open_handle()?;

    // mount! the user database as "main" (if specified)
    if let Some(ref path) = db_path {
        let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
        crate::exec_ng::run_dql_query(&format!("mount!(\"{}\", \"main\")", path), &mut *session)?;
    }

    if !attach.is_empty() {
        crate::attach::process_attach_flags(&mut *handle, attach)?;
    }

    if let Some(ref q) = query {
        run_query(
            q,
            &mut *handle,
            to.clone(),
            output_format,
            *no_headers,
            *no_sanitize,
            *sequential,
        )
    } else if let Some(ref f) = file {
        let source_code = std::fs::read_to_string(f)?;
        run_query(
            &source_code,
            &mut *handle,
            to.clone(),
            output_format,
            *no_headers,
            *no_sanitize,
            *sequential,
        )
    } else if !io::stdin().is_terminal() {
        let mut buffer = String::new();
        io::stdin().read_to_string(&mut buffer)?;
        if buffer.trim().is_empty() {
            anyhow::bail!("No input provided via stdin");
        }
        run_query(
            &buffer,
            &mut *handle,
            to.clone(),
            output_format,
            *no_headers,
            *no_sanitize,
            *sequential,
        )
    } else {
        #[cfg(feature = "repl")]
        {
            crate::repl::run_interactive_with_connection(
                db_path,
                output_format,
                *quiet,
                highlights.as_deref(),
                Some(conn),
            )
        }
        #[cfg(not(feature = "repl"))]
        {
            anyhow::bail!("Interactive REPL mode requires the 'repl' feature")
        }
    }
}
