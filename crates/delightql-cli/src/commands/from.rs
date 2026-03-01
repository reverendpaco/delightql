//! From command handler
//!
//! Handles: dql from json "query"
//! Reads data from stdin and makes it available as special.input table

use crate::args::{CliArgs, Command};
use crate::connection;
use crate::output_format::OutputFormat;
use anyhow::Result;
use std::io::{self, IsTerminal};
use std::path::Path;

/// Handle from subcommand
pub fn handle_from_subcommand(command: &Command, base_args: &CliArgs) -> Result<()> {
    let (
        format,
        query,
        file,
        format_out,
        to,
        _assert_queries,
        _if_errors_query,
        _debug,
        sql_optimize,
        inline_ctes,
        no_headers,
        no_sanitize,
        _strict,
        _quiet,
        make_new_db_if_missing,
        consult_files,
        attach,
    ) = match command {
        Command::From {
            format,
            query,
            file,
            format_out,
            to,
            assert_queries,
            if_errors_query,
            debug,
            sql_optimize,
            inline_ctes,
            no_headers,
            no_sanitize,
            strict,
            quiet,
            make_new_db_if_missing,
            consult_files,
            attach,
        } => (
            format,
            query,
            file,
            format_out,
            to,
            assert_queries,
            if_errors_query,
            debug,
            sql_optimize,
            inline_ctes,
            no_headers,
            no_sanitize,
            strict,
            quiet,
            make_new_db_if_missing,
            consult_files,
            attach,
        ),
        _ => unreachable!("handle_from_subcommand called with non-From command"),
    };

    if *sql_optimize > 0 {
        eprintln!("warning: --soptimize currently does nothing");
    }
    if *inline_ctes {
        eprintln!("warning: --inline-ctes currently does nothing");
    }

    if io::stdin().is_terminal() {
        anyhow::bail!(
            "dql from requires data piped to stdin. Usage: cat data.{} | dql from {}",
            format,
            format
        );
    }

    let special_db_path = crate::stdin_relation::create_special_database(format)?;

    let query_text = if let Some(ref q) = query {
        q.clone()
    } else if let Some(ref f) = file {
        std::fs::read_to_string(f)
            .map_err(|e| anyhow::anyhow!("Failed to read query file {}: {}", f.display(), e))?
    } else {
        anyhow::bail!("Query required when using 'dql from'. Provide query string or use --file.");
    };

    let output_format = format_out.clone().unwrap_or(OutputFormat::Table);

    let db_path = base_args
        .database
        .as_ref()
        .map(|p| p.to_string_lossy().to_string());

    if let Some(ref path) = db_path {
        if !*make_new_db_if_missing && !Path::new(path).exists() {
            anyhow::bail!(
                "Database file '{}' does not exist. Use --make-new-db-if-missing to create it.",
                path
            );
        }
        if path.starts_with("pipe://") {
            anyhow::bail!("'dql from' command not supported for pipe connections");
        }
    }

    if !consult_files.is_empty() {
        anyhow::bail!("--consult flag not supported. Use consult!() in DQL source instead.");
    }

    let conn = connection::ConnectionManager::new_memory()?;
    let mut handle = conn.open_handle()?;

    // mount! the user database as "main" (if specified), then special db
    {
        let mut mount_session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;

        if let Some(ref path) = db_path {
            crate::exec_ng::run_dql_query(
                &format!("mount!(\"{}\", \"main\")", path),
                &mut *mount_session,
            )?;
        }

        // Attach the special stdin database
        let special_path_str = special_db_path.to_string_lossy().to_string();
        crate::exec_ng::run_dql_query(
            &format!("mount!(\"{}\", \"special\")", special_path_str),
            &mut *mount_session,
        )?;
    }

    if !attach.is_empty() {
        crate::attach::process_attach_flags(&mut *handle, attach)?;
    }

    let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;

    let result = crate::exec_ng::execute_query(
        &query_text,
        &mut *session,
        to.clone(),
        output_format,
        *no_headers,
        *no_sanitize,
        false,
    );

    let _ = std::fs::remove_file(&special_db_path);

    result.map(|_| ())
}
