/// DelightQL CLI
///
/// Command-line interface for the DelightQL query language
use anyhow::Result;
use clap::Parser;

use delightql_cli::args::CliArgs;
use delightql_cli::output_format::OutputFormat;
use delightql_cli::{args, exec};

// Thread-local storage for assert queries and error formatting
thread_local! {
    static ASSERT_QUERIES: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
    static IF_ERRORS_QUERY: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
    static CLI_FLAGS: std::cell::RefCell<Option<CliFlags>> = const { std::cell::RefCell::new(None) };
}

/// CLI flags needed for error formatting
#[derive(Clone)]
struct CliFlags {
    output_format: OutputFormat,
    _to: Option<args::Stage>,
    no_headers: bool,
    error_prefix: String,
}
fn main() {
    // Reset SIGPIPE to default so piping to `head`, `tail`, etc. exits cleanly
    // instead of panicking on broken pipe.
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }

    stacksafe::set_minimum_stack_size(512 * 1024);

    let result = run();

    // Check if we have assertion queries
    let has_assertions = ASSERT_QUERIES.with(|aq| !aq.borrow().is_empty());

    if has_assertions {
        // Always run assertions (regardless of success/failure)
        // Diagnostics DB is always created by core::Pipeline
        if let Err(assert_err) = run_assertions() {
            eprintln!("Assertion failed: {}", assert_err);
            std::process::exit(1);
        }
        // All assertions passed - exit 0
        std::process::exit(0);
    }

    // No assertions - normal flow
    if let Err(e) = result {
        // Check if we have custom error formatting
        let if_errors_query = IF_ERRORS_QUERY.with(|q| q.borrow().clone());

        if let Some(query) = if_errors_query {
            // Custom error formatting - run query and output to stdout
            let flags = CLI_FLAGS
                .with(|f| f.borrow().clone())
                .unwrap_or_else(|| CliFlags {
                    output_format: OutputFormat::Table,
                    _to: None,
                    no_headers: false,
                    error_prefix: "\x1E".to_string(),
                });

            if let Err(format_err) =
                format_errors_with_query(&query, flags.output_format, flags.no_headers)
            {
                eprintln!("Error formatting failed: {}", format_err);
                eprintln!("Original error: {}", e);
            }
            std::process::exit(1);
        } else {
            // Normal error reporting to stderr
            // Structured error lines get the configured prefix (default: RS \x1E)
            let prefix = CLI_FLAGS
                .with(|f| f.borrow().as_ref().map(|fl| fl.error_prefix.clone()))
                .unwrap_or_else(|| "\x1E".to_string());

            let error_display = format!("{}", e);
            if let Some(dql_err) = e.downcast_ref::<delightql_core::error::DelightQLError>() {
                eprintln!("{}[{}] {}", prefix, dql_err.error_uri(), e);
            } else if error_display.starts_with('[') {
                // Identity prefix from protocol error: "[dql/parse/general] Syntax: ..."
                eprintln!("{}{}", prefix, error_display);
            } else {
                eprintln!("{}Error: {}", prefix, e);
            }
            std::process::exit(1);
        }
    }
}

/// Format errors using custom DelightQL query - outputs to stdout
fn format_errors_with_query(
    query: &str,
    output_format: OutputFormat,
    no_headers: bool,
) -> anyhow::Result<()> {
    let diagnostics_db = std::env::temp_dir().join("delightql_diagnostics.db");
    if !diagnostics_db.exists() {
        anyhow::bail!("No diagnostics database found");
    }

    exec::execute_query(
        query,
        Some(diagnostics_db.to_string_lossy().to_string()),
        output_format,
        None,
        no_headers,
    )?;

    let _ = std::fs::remove_file(&diagnostics_db);
    Ok(())
}

/// Run ALL assertion queries against diagnostics database (ANDed together)
/// Returns Ok if ALL queries return rows, Err if ANY query returns no rows
fn run_assertions() -> anyhow::Result<()> {
    let assert_queries = ASSERT_QUERIES.with(|aq| aq.borrow().clone());

    if assert_queries.is_empty() {
        anyhow::bail!("No assertion queries set");
    }

    // Check if diagnostics database exists
    let diagnostics_db = std::env::temp_dir().join("delightql_diagnostics.db");
    if !diagnostics_db.exists() {
        anyhow::bail!("No diagnostics database found - pipeline may not have run");
    }

    for (i, assert_query) in assert_queries.iter().enumerate() {
        let result = exec::execute_query(
            assert_query,
            Some(diagnostics_db.to_string_lossy().to_string()),
            OutputFormat::Table,
            None,
            true, // no_headers for assertions
        )?;

        // Check if this assertion passed
        let passed = if let Some(metadata) = result {
            metadata.row_count > 0
        } else {
            false
        };

        if !passed {
            // Clean up temp file before failing
            let _ = std::fs::remove_file(&diagnostics_db);
            anyhow::bail!(
                "Assertion #{} failed (returned no rows): {}",
                i + 1,
                assert_query
            );
        }
    }

    // Clean up temp file after all assertions pass
    let _ = std::fs::remove_file(&diagnostics_db);

    // All assertions passed!
    Ok(())
}

#[stacksafe::stacksafe]
fn run() -> Result<()> {
    // Initialize logger from RUST_LOG environment variable
    env_logger::init();

    // Parse command-line arguments
    let args = CliArgs::parse();

    // Store error prefix for use in main() error handler
    CLI_FLAGS.with(|f| {
        *f.borrow_mut() = Some(CliFlags {
            output_format: OutputFormat::Table,
            _to: None,
            no_headers: false,
            error_prefix: args.error_prefix.clone(),
        });
    });

    // PHASE 1: Check for subcommands FIRST (new interface)
    if let Some(ref command) = args.command {
        use delightql_cli::args::{Command, ToolCommand};

        return match command {
            Command::Query { .. } => {
                delightql_cli::commands::query::handle_query_subcommand(command, &args)
            }
            Command::From { .. } => {
                delightql_cli::commands::from::handle_from_subcommand(command, &args)
            }
            Command::Format { .. } => {
                delightql_cli::commands::format::handle_format_subcommand(command, &args)
            }
            Command::Server {
                socket,
                workers,
                idle_timeout,
            } => {
                let db_path = args
                    .database
                    .as_ref()
                    .map(|p| p.to_string_lossy().to_string());

                let socket_path = socket.clone().unwrap_or_else(|| {
                    std::path::PathBuf::from(format!("/tmp/dql-{}.sock", std::process::id()))
                });

                let num_workers = if *workers == 0 {
                    std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(4)
                } else {
                    *workers
                };

                let idle = if *idle_timeout > 0 {
                    Some(*idle_timeout)
                } else {
                    None
                };
                delightql_cli::server::start_server(
                    db_path.as_deref(),
                    &socket_path,
                    num_workers,
                    idle,
                )
            }
            Command::Tools { tool } => match tool {
                ToolCommand::Jstruct {
                    query,
                    format,
                    to,
                } => delightql_cli::commands::jstruct::handle_jstruct_command(
                    query, *format, *to, &args,
                ),
                ToolCommand::Munge { .. } => {
                    anyhow::bail!("dql tools munge is not yet implemented")
                }
            },
        };
    }

    // No subcommand provided - start REPL
    #[cfg(feature = "repl")]
    {
        let db_path = args.database.map(|p| p.to_string_lossy().to_string());
        return delightql_cli::repl::run_interactive(
            db_path,
            delightql_cli::output_format::OutputFormat::resolve(None),
            false, // quiet
            None,  // highlights_path
        );
    }

    #[cfg(not(feature = "repl"))]
    anyhow::bail!(
        "No subcommand specified. Use one of: query, from, format, server\n\
         Examples:\n\
         - dql query \"users(*)\"\n\
         - dql query --file query.dql\n\
         - echo '[...]' | dql from json 'special.input(*)'\n\
         - dql format < query.dql\n\
         - dql server\n\n\
         Run 'dql --help' for more information.\n\n\
         Note: REPL is not available - this binary was built without the 'repl' feature."
    )
}

#[cfg(test)]
mod tests {
    use escargot;
    use std::fs;
    use tempfile::NamedTempFile;

    fn get_cli_path() -> std::path::PathBuf {
        // When running tests, we need to ensure the binary is built
        // Use escargot to get the path to the binary

        // Set the manifest directory explicitly to avoid current directory issues
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let manifest_path = std::path::Path::new(manifest_dir).join("Cargo.toml");

        // Change to a valid directory before running escargot
        let original_dir = std::env::current_dir().ok();
        let _ = std::env::set_current_dir(manifest_dir);

        let result = escargot::CargoBuild::new()
            .bin("dql")
            .manifest_path(&manifest_path)
            .current_release()
            .current_target()
            .run()
            .unwrap()
            .path()
            .to_path_buf();

        // Restore original directory if we had one
        if let Some(dir) = original_dir {
            let _ = std::env::set_current_dir(dir);
        }

        result
    }

    #[test]
    fn test_cli_help() {
        // Test that help flag works
        let cli_path = get_cli_path();
        let result = std::process::Command::new(cli_path).arg("--help").output();

        assert!(result.is_ok(), "Failed to run command: {:?}", result.err());
        let output = result.unwrap();
        assert!(
            output.status.success(),
            "Command failed with status: {:?}",
            output.status
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("DelightQL"),
            "Output doesn't contain 'DelightQL': {}",
            stdout
        );
        assert!(
            stdout.contains("Query language transpiler"),
            "Output doesn't contain expected text: {}",
            stdout
        );
    }

    #[test]
    fn test_cli_version() {
        // Test that version flag works
        let cli_path = get_cli_path();
        let result = std::process::Command::new(cli_path)
            .arg("--version")
            .output();

        assert!(result.is_ok(), "Failed to run command: {:?}", result.err());
        let output = result.unwrap();
        assert!(
            output.status.success(),
            "Command failed with status: {:?}",
            output.status
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("delightql"),
            "Output doesn't contain 'delightql': {}",
            stdout
        );
    }
}
