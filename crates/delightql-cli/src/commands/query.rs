// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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
    // URI schemes are not files; their reachability (or refusal, for
    // unsupported schemes) is handled by the connection path — never by
    // file existence, and never by --make-new-db-if-missing.
    if connection::looks_like_uri(db_path) {
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

/// A plain-file `--db` target is CREATE intent when `--make-new-db-if-missing`
/// is set and the path is missing or a 0-byte stub: the primary mount then
/// routes through `mount_new!` (which materializes a VALID empty database)
/// rather than the attach-only `mount!` (EFFECT-ALGEBRA §6 — `mount!`
/// rejects missing/empty/invalid files). An
/// existing non-empty database, or any URI, keeps `mount!`.
/// Pinned by tests/mount_validation.rs::make_new_db_if_missing_creates_and_works.
fn wants_provision(db_path: &str, make_new_db_if_missing: bool) -> bool {
    if !make_new_db_if_missing || connection::looks_like_uri(db_path) {
        return false;
    }
    match std::fs::metadata(db_path) {
        Ok(m) => m.len() == 0, // 0-byte stub → provision
        Err(_) => true,        // missing → provision
    }
}

fn make_connection(
    db_path: &Option<String>,
    make_new_db_if_missing: bool,
    via: Option<&str>,
) -> Result<connection::ConnectionManager> {
    if let Some(ref path) = db_path {
        check_database_exists(path, make_new_db_if_missing)?;
        connection::ConnectionManager::open(path, via)
    } else {
        connection::ConnectionManager::new_memory()
    }
}

/// Execute a query string: create session via handle, call exec_ng.
///
/// Human-output modes (`--to results` / default) get the console
/// sink: mid-run `stdout!` result sets print live as the run executes
/// (EFFECT-ALGEBRA §5; the run's return value still arrives as the ordinary
/// result). Machine modes (`--to hash`, `--to sql`, …) install NO sink so
/// their output stays a single machine-readable value — both halves pinned
/// by `tests/stdout_ship.rs`.
fn run_query(
    source: &str,
    handle: &mut dyn delightql_core::api::DqlHandle,
    to: Option<args::Stage>,
    output_format: OutputFormat,
    no_headers: bool,
    no_sanitize: bool,
    sequential: bool,
) -> Result<()> {
    let console_sink = matches!(to, None | Some(args::Stage::Results));
    let hooks = if console_sink {
        delightql_core::api::SessionHooks {
            on_ship: Some(Box::new(
                move |columns: &[String], rows: &[Vec<Option<Vec<u8>>>]| {
                    // The display boundary: cells become text HERE, where
                    // the choice of what to print for an absent one is the
                    // console's to make.
                    let display_rows: Vec<Vec<String>> = rows
                        .iter()
                        .map(|row| crate::exec_ng::cells_to_display(row))
                        .collect();
                    let output = crate::output_format::format_output_with_zebra(
                        columns,
                        &display_rows,
                        output_format,
                        None,
                        no_headers,
                        no_sanitize,
                    );
                    print!("{}", output);
                },
            )),
        }
    } else {
        delightql_core::api::SessionHooks::default()
    };
    let mut session = handle
        .session_with_hooks(hooks)
        .map_err(|e| anyhow::anyhow!("{}", e))?;
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

/// The road a `query` invocation is on: the prompt when there is no query,
/// no file, and a terminal on stdin; one-shot otherwise. The ONE place
/// that decides it — the process database and the dispatch both ask here.
pub fn mode_of(command: &Command) -> crate::client::context::Mode {
    use crate::client::context::Mode;
    match command {
        // The replay driver is neither prompt nor one-shot: it types into
        // a child that is the prompt.
        #[cfg(feature = "repl")]
        Command::Query {
            replay_repl: Some(_),
            ..
        } => Mode::Other,
        Command::Query { query, file, .. }
            if query.is_none() && file.is_none() && io::stdin().is_terminal() =>
        {
            Mode::Repl
        }
        Command::Query { .. } => Mode::Query,
        _ => Mode::Other,
    }
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
        attach,
        sequential,
        dangers,
        #[cfg(feature = "repl")]
        quiet,
        #[cfg(feature = "repl")]
        highlights,
        #[cfg(feature = "repl")]
        replay_repl,
        ..
    } = command
    else {
        unreachable!("handle_query_subcommand called with non-Query command")
    };

    // The replay driver: no connection, no handle of its own — it spawns
    // this executable on a pty and types the recorded session into it.
    #[cfg(feature = "repl")]
    if let Some(source) = replay_repl {
        let argv: Vec<String> = std::env::args().collect();
        let fallback = crate::client::replay::arguments_without_replay(&argv);
        let code = crate::client::replay::run(source, &fallback)?;
        crate::client::exit::finish(None, code);
        crate::client::exit::announce();
        std::process::exit(code);
    }

    let output_format = OutputFormat::resolve(format.clone());
    let db_path = base_args
        .database
        .as_ref()
        .map(|p| p.to_string_lossy().to_string());

    if *no_sanitize {
        crate::client::incident::warning("argument", crate::client::incident::hierarchy::SANITIZE_DISABLED, "output sanitization disabled, terminal injection possible".to_string());
    }

    // Build the connection manager. For a fatboy target this classifies the
    // route WITHOUT spawning a child (fatboy_exec: FatboyManager is lazy);
    // for SQLite it opens (and, with --make-new-db-if-missing, creates) the
    // file. It also validates existence/provisioning intent. The session's
    // one backend is created by the mount! below (one-shot) or by the REPL's
    // own mount (interactive) — never by this manager directly.
    let conn = make_connection(
        &db_path,
        base_args.make_new_db_if_missing,
        base_args.via.as_deref(),
    )?;

    // Interactive REPL: it builds AND mounts its own session
    // (repl::run_interactive_with_connection → new_with_connection), so
    // opening a handle and mounting here would create a backend only to
    // discard it. Hand the manager over and let the REPL be the SOLE
    // backend-opener on its path — one child, mirroring the one-shot mount!
    // (pinned indirectly by the
    // REPL smoke path — no ball coverage).
    if mode_of(command) == crate::client::context::Mode::Repl {
        #[cfg(feature = "repl")]
        {
            return crate::repl::run_interactive_with_connection(
                db_path,
                output_format,
                *quiet,
                highlights.as_deref(),
                Some(conn),
            );
        }
        #[cfg(not(feature = "repl"))]
        {
            anyhow::bail!("Interactive REPL mode requires the 'repl' feature")
        }
    }

    let mut handle = connection::open_handle()?;

    // Apply --danger session-baseline overrides. Core parses and
    // validates the textual specs — unknown gates, bad states, and
    // non-CLI-overridable (semantic) gates refuse with teaching errors;
    // a --danger that cannot take effect refuses, never no-ops.
    if !dangers.is_empty() {
        handle
            .set_danger_overrides(&dangers)
            .map_err(|e| anyhow::anyhow!("{}", e))?;
    }

    // mount! the user database as "main" (if specified). Under
    // --make-new-db-if-missing a missing/empty target is CREATE intent and
    // routes through mount_new! (see wants_provision). This is the sole
    // backend-creating step on the one-shot path.
    if let Some(ref path) = db_path {
        let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
        let directive = if wants_provision(path, base_args.make_new_db_if_missing) {
            format!("mount_new!(\"{}\", \"main\")(*)", path)
        } else {
            format!("mount!(\"{}\", \"main\")(*)", path)
        };
        crate::exec_ng::run_dql_query(&directive, &mut *session)?;
    }

    if !attach.is_empty() {
        crate::attach::process_attach_flags(&mut *handle, attach)?;
    }

    // Run, then close the session while the handle — and core's findings
    // in it — are alive, then answer. `?` would skip the close on error,
    // which is the case the session files exist for.
    let result = run_one_shot(
        query.as_deref(),
        file.as_deref(),
        &mut *handle,
        to.clone(),
        output_format,
        *no_headers,
        *no_sanitize,
        *sequential,
    );
    crate::client::exit::finish(Some(&mut *handle), if result.is_ok() { 0 } else { 1 });
    result
}

#[allow(clippy::too_many_arguments)]
fn run_one_shot(
    query: Option<&str>,
    file: Option<&std::path::Path>,
    handle: &mut dyn delightql_core::api::DqlHandle,
    to: Option<crate::args::Stage>,
    output_format: OutputFormat,
    no_headers: bool,
    no_sanitize: bool,
    sequential: bool,
) -> Result<()> {
    if let Some(q) = query {
        run_query(
            q,
            handle,
            to,
            output_format,
            no_headers,
            no_sanitize,
            sequential,
        )
    } else if let Some(f) = file {
        // `--file -` means stdin, per convention (R2.4) — otherwise the
        // OS goes looking for a file literally named "-".
        let source_code = if f.as_os_str() == "-" {
            let mut buffer = String::new();
            io::stdin().read_to_string(&mut buffer)?;
            buffer
        } else {
            std::fs::read_to_string(f)?
        };
        run_query(
            &source_code,
            handle,
            to,
            output_format,
            no_headers,
            no_sanitize,
            sequential,
        )
    } else {
        // No query, no file, and (per the interactive check above) stdin is
        // NOT a terminal: read the piped program from stdin. ANNOUNCE the
        // wait first — a pipe that never delivers EOF (a common agent/CI
        // wiring) otherwise blocks here with zero output, and the eventual
        // timeout-kill leaves no evidence of why. Every state the tool
        // enters must emit evidence of that state on stderr; stdout stays
        // machine-clean.
        eprintln!(
            "reading query from stdin (end with EOF; or pass the query as \
             an argument, or --file <path>)"
        );
        let mut buffer = String::new();
        io::stdin().read_to_string(&mut buffer)?;
        if buffer.trim().is_empty() {
            anyhow::bail!("No input provided via stdin");
        }
        run_query(
            &buffer,
            handle,
            to,
            output_format,
            no_headers,
            no_sanitize,
            sequential,
        )
    }
}
