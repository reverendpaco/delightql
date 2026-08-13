// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// DelightQL CLI
///
/// Command-line interface for the DelightQL query language
use anyhow::Result;
use clap::Parser;

use delightql_cli::args;
use delightql_cli::args::CliArgs;
use delightql_cli::output_format::OutputFormat;

// Thread-local storage for error formatting.
// (--assert / --if-errors are deliberately absent: the in-language
// assertion hooks are the replacement — CLI flags here would be dead
// wiring.)
thread_local! {
    static CLI_FLAGS: std::cell::RefCell<Option<CliFlags>> = const { std::cell::RefCell::new(None) };
}

/// --error-format json: an AtomicBool, not a thread-local,
/// because the panic hook may fire on any thread (server workers).
static ERROR_FORMAT_JSON: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Emit one structured error record on stderr. Text mode: the classic
/// `<prefix>[uri] message` / `<prefix>Error: message`. Json mode: with
/// the default RS prefix this is exactly RFC 7464 (RS + JSON + LF, what
/// `jq --seq` reads); `uri` is null for unbadged errors. Additive
/// fields are legal later (the plumbing is VERSIONED).
fn emit_error_record(prefix: &str, uri: Option<&str>, message: &str) {
    if ERROR_FORMAT_JSON.load(std::sync::atomic::Ordering::Relaxed) {
        use delightql_cli::output_format::json_escape;
        let uri_json = match uri {
            Some(u) => json_escape(u),
            None => "null".to_string(),
        };
        eprintln!(
            "{}{{\"uri\": {}, \"message\": {}}}",
            prefix,
            uri_json,
            json_escape(message)
        );
    } else {
        match uri {
            Some(u) => eprintln!("{prefix}[{u}] {message}"),
            None => eprintln!("{prefix}Error: {message}"),
        }
    }
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

    // A panic must never reach the user as rc=101 + backtrace dump:
    // the hook emits a structured internal/panic record on whatever
    // thread panicked (so server
    // workers are covered too), and RUST_BACKTRACE chains the default
    // hook for debugging. catch_unwind below converts the main-path
    // unwind to exit 1 — the record has already been printed.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let payload = info.payload();
        let msg = if let Some(s) = payload.downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = payload.downcast_ref::<String>() {
            s.clone()
        } else {
            "panic with non-string payload".to_string()
        };
        let at = info
            .location()
            .map(|l| format!(" (at {}:{})", l.file(), l.line()))
            .unwrap_or_default();
        let prefix = CLI_FLAGS
            .with(|f| f.borrow().as_ref().map(|fl| fl.error_prefix.clone()))
            .unwrap_or_else(|| "\x1E".to_string());
        emit_error_record(
            &prefix,
            Some("delightql-error://internal/panic"),
            &format!(
                "{msg}{at} — this is a dql bug, please report it \
                 (`dql explain internal/panic`; RUST_BACKTRACE=1 for a backtrace)"
            ),
        );
        if std::env::var_os("RUST_BACKTRACE").is_some() {
            default_hook(info);
        }
    }));

    let result = match std::panic::catch_unwind(run) {
        Ok(r) => r,
        Err(_) => std::process::exit(1), // record already emitted by the hook
    };

    if let Err(e) = result {
        // Normal error reporting to stderr
        // Structured error lines get the configured prefix (default: RS \x1E)
        let prefix = CLI_FLAGS
            .with(|f| f.borrow().as_ref().map(|fl| fl.error_prefix.clone()))
            .unwrap_or_else(|| "\x1E".to_string());

        let error_display = format!("{}", e);
        if let Some(dql_err) = e.downcast_ref::<delightql_core::error::DelightQLError>() {
            emit_error_record(&prefix, Some(&dql_err.error_uri()), &error_display);
        } else if let Some(rest) = error_display
            .strip_prefix('[')
            .and_then(|r| r.split_once("] "))
        {
            // Identity prefix from protocol error: "[dql/parse/general] Syntax: ..."
            emit_error_record(&prefix, Some(rest.0), rest.1);
        } else {
            emit_error_record(&prefix, None, &error_display);
        }
        std::process::exit(1);
    }
}

#[stacksafe::stacksafe]
fn run() -> Result<()> {
    // Initialize logger from RUST_LOG environment variable
    env_logger::init();

    // Parse command-line arguments
    let args = CliArgs::parse();

    // --error-format json: set the process-wide switch before
    // anything can fail or panic.
    ERROR_FORMAT_JSON.store(
        args.error_format == args::ErrorFormat::Json,
        std::sync::atomic::Ordering::Relaxed,
    );

    // Test hook for the panic catch itself: the bin test pins the
    // contract (a panic exits 1 with a structured internal/panic
    // record, never rc=101 + backtrace). Hidden, test-only. Sits
    // after flag parsing so the record honors --error-format.
    if std::env::var_os("DQL_TEST_PANIC").is_some() {
        panic!("deliberate test panic (DQL_TEST_PANIC)");
    }

    // Global-option hygiene: a global flag the chosen subcommand
    // cannot consume is refused, not ignored — accepted-and-ignored
    // input is the silent-wrong of argument parsing (same family as
    // the old `--dialect oracle` warn-and-continue). Flags stay
    // clap-global so both argument orders keep working for the
    // subcommands that DO consume them. No subcommand = bare query
    // (REPL on a TTY, execute on a pipe), which consumes all three.
    if let Some(ref command) = args.command {
        use delightql_cli::args::Command;
        let (name, db_ok, dialect_ok, via_ok) = match command {
            Command::Query { .. } => ("query", true, true, true),
            Command::Server { .. } => ("server", true, true, false),
            Command::Tools { .. } => ("tools", false, true, false),
            Command::Version { .. } => ("version", false, false, false),
            Command::Format { .. } => ("format", false, false, false),
            Command::Explain { .. } => ("explain", false, false, false),
            Command::Target { .. } => ("target", false, false, false),
            Command::Man { .. } => ("man", false, false, false),
            Command::Book { .. } => ("book", true, false, false),
            Command::Editor { .. } => ("editor", false, false, false),
            Command::Help { .. } => ("help", false, false, false),
            Command::Completions { .. } => ("completions", false, false, false),
            Command::Selftest { .. } => ("selftest", false, false, false),
        };
        // --make-new-db-if-missing is --db's companion knob, but only
        // query creates databases (server's --db must already exist).
        let mknew_ok = matches!(command, Command::Query { .. });
        for (flag, provided, ok) in [
            ("--db", args.database.is_some(), db_ok),
            (
                "--make-new-db-if-missing",
                args.make_new_db_if_missing,
                mknew_ok,
            ),
            ("--dialect", args.dialect.is_some(), dialect_ok),
            ("--via", args.via.is_some(), via_ok),
        ] {
            if provided && !ok {
                anyhow::bail!(
                    "{flag} means nothing to 'dql {name}' — refusing rather \
                     than silently ignoring it"
                );
            }
        }
    }

    // --dialect is sugar for DQL_DIALECT (consumed at pipeline construction).
    // The flag is clap-validated (args.rs::parse_dialect); an externally set
    // DQL_DIALECT is validated here, once, loudly: an ignored explicit
    // override is a silent-wrong — same principle as the DQL_FATBOY_DIR
    // hard pin.
    if let Some(ref dialect) = args.dialect {
        std::env::set_var("DQL_DIALECT", dialect);
    } else if let Ok(v) = std::env::var("DQL_DIALECT") {
        if !delightql_core::is_known_dialect_family(v.trim()) {
            anyhow::bail!(
                "unknown DQL_DIALECT '{}'. Valid dialects: sqlite, postgres \
                 (alias: postgresql), mysql, sqlserver, duckdb — unset the \
                 variable to derive the dialect from the connection",
                v
            );
        }
    }

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
            Command::Version { json } => {
                // The self-hash distinguishes builds the identity contract
                // cannot: every from-source build reports "dev".
                let self_hash = delightql_cli::version_info::binary_sha256();
                if *json {
                    // buildinfo's json() is hand-rolled (the crate is
                    // dependency-free by charter); splice the additive key
                    // here rather than teach buildinfo about hashing.
                    let base = delightql_buildinfo::json();
                    let base = base.trim_end_matches('}');
                    let hash_field = match &self_hash {
                        Some(h) => format!("\"{}\"", h),
                        None => "null".to_string(),
                    };
                    println!("{},\"binary_sha256\":{}}}", base, hash_field);
                } else {
                    println!("{}", delightql_buildinfo::human());
                    if let Some(h) = self_hash {
                        println!("binary sha256: {}", h);
                    }
                }
                Ok(())
            }
            Command::Query { .. } => {
                delightql_cli::commands::query::handle_query_subcommand(command, &args)
            }
            Command::Explain { identifier } => {
                delightql_cli::commands::explain::handle_explain(identifier)
            }
            Command::Man { name, dump } => {
                delightql_cli::commands::man::handle_man(name, dump.as_deref())
            }
            Command::Book {
                name,
                export_images,
            } => delightql_cli::commands::book::handle_book(
                name.as_deref(),
                export_images.as_deref(),
                args.database.as_deref(),
            ),
            Command::Editor { action } => match action {
                delightql_cli::args::EditorCommand::ExportArtifacts { dir } => {
                    delightql_cli::commands::editor::handle_export_artifacts(dir)
                }
            },
            // `dql help <cmd>` is the SAME projection as `dql man
            // <cmd>` — one mechanism, not a clap essay and a man page
            // drifting apart. Bare `dql help` keeps the usage summary.
            Command::Help { name } => {
                if name.is_empty() {
                    <CliArgs as clap::CommandFactory>::command().print_long_help()?;
                    Ok(())
                } else {
                    delightql_cli::commands::man::handle_man(name, None)
                }
            }
            Command::Completions { shell } => {
                let mut cmd = <CliArgs as clap::CommandFactory>::command();
                clap_complete::generate(*shell, &mut cmd, "dql", &mut std::io::stdout());
                Ok(())
            }
            Command::Selftest { json, strict } => {
                delightql_cli::commands::selftest::handle_selftest(*json, *strict)
            }
            Command::Target { action } => match action {
                delightql_cli::args::TargetCommand::List => {
                    delightql_cli::commands::target::handle_target_list()
                }
                delightql_cli::args::TargetCommand::Install { profile, from } => {
                    delightql_cli::commands::target::handle_target_install(profile, from.as_deref())
                }
                delightql_cli::args::TargetCommand::Verify => {
                    delightql_cli::commands::target::handle_target_verify()
                }
            },
            Command::Format { .. } => {
                delightql_cli::commands::format::handle_format_subcommand(command, &args)
            }
            Command::Server {
                socket,
                workers,
                idle_timeout,
                socket_idle_timeout,
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
                let socket_idle = if *socket_idle_timeout > 0 {
                    Some(*socket_idle_timeout)
                } else {
                    None
                };
                delightql_cli::server::start_server(
                    db_path.as_deref(),
                    &socket_path,
                    num_workers,
                    idle,
                    socket_idle,
                )
            }
            Command::Tools { tool } => match tool {
                ToolCommand::Jstruct { query, format, to } => {
                    delightql_cli::commands::jstruct::handle_jstruct_command(
                        query, *format, *to, &args,
                    )
                }
                ToolCommand::Csvstruct {
                    query,
                    format,
                    to,
                    has_headers,
                    delimiter,
                } => delightql_cli::commands::csvstruct::handle_csvstruct_command(
                    query,
                    *format,
                    *to,
                    *has_headers,
                    delimiter,
                    &args,
                ),
                ToolCommand::Filemunge {
                    query,
                    tables,
                    format,
                    to,
                } => delightql_cli::commands::filemunge::handle_filemunge_command(
                    query, tables, *format, *to, &args,
                ),
            },
        };
    }

    // No subcommand: bare `dql` is sugar for `dql query` with no
    // arguments — one rule, one code path. On a terminal that means
    // the REPL; with piped stdin it means read-and-execute. The
    // tempting regression: bare dql printing the REPL banner,
    // DISCARDing piped input, and exiting 0 — the silent-wrong of
    // entrances. Parsing the literal words
    // makes the equivalence true by construction rather than by a
    // hand-mirrored field list; the user's real global flags still
    // arrive via `args`.
    let bare_query = <CliArgs as clap::Parser>::parse_from(["dql", "query"])
        .command
        .expect("'dql query' parses to the Query subcommand");
    delightql_cli::commands::query::handle_query_subcommand(&bare_query, &args)
}

#[cfg(test)]
mod tests {
    use escargot;
    use std::fs;
    use tempfile::NamedTempFile;

    /// The weld between the formatter's knob registry and the burned
    /// sys::format.bundle table: same column set, and the 'book' row
    /// carries exactly FormatConfig::default()'s values. A knob added
    /// on either side goes red here until the other side follows.
    #[cfg(feature = "formatter")]
    #[test]
    fn sys_format_bundle_welds_to_knob_registry() {
        let cli_path = get_cli_path();
        let out = std::process::Command::new(&cli_path)
            .args([
                "query",
                "sys::format.bundle(*), bundle = \"book\"",
                "-f",
                "csv",
            ])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "querying sys::format.bundle failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        let mut lines = stdout.lines();
        let header: Vec<&str> = lines.next().expect("header row").split(',').collect();
        let book: Vec<&str> = lines.next().expect("book row").split(',').collect();

        let mut expected_cols = vec!["bundle"];
        expected_cols.extend(delightql_formatter::KNOBS.iter().map(|k| k.name));
        let mut sorted_header = header.clone();
        sorted_header.sort_unstable();
        let mut sorted_expected = expected_cols.clone();
        sorted_expected.sort_unstable();
        assert_eq!(
            sorted_header, sorted_expected,
            "sys::format.bundle columns must equal the knob registry (plus 'bundle')"
        );

        let defaults = delightql_formatter::FormatConfig::default();
        for (col, value) in header.iter().zip(book.iter()) {
            if *col == "bundle" {
                assert_eq!(*value, "book");
                continue;
            }
            let knob = delightql_formatter::KNOBS
                .iter()
                .find(|k| k.name == *col)
                .expect("column checked above");
            assert_eq!(
                *value,
                (knob.get)(&defaults),
                "burned 'book' value for {col} drifted from FormatConfig::default()"
            );
        }
    }

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

    /// The CLI surface fixes stay fixed.
    #[test]
    fn test_cli_surface_fixes_2026_07_05() {
        let cli_path = get_cli_path();
        let run = |args: &[&str]| {
            std::process::Command::new(&cli_path)
                .env_remove("DQL_DIALECT")
                .args(args)
                .output()
                .unwrap()
        };

        // --to <stage> -f raw prints the bare artifact; never panics.
        for stage in ["sql", "cst", "ast-refined"] {
            let out = run(&["query", "--to", stage, "-f", "raw", "_(a @ 1)"]);
            assert!(
                out.status.success(),
                "--to {stage} -f raw: {:?}\n{}",
                out.status,
                String::from_utf8_lossy(&out.stderr)
            );
        }
        let out = run(&["query", "--to", "sql", "-f", "raw", "_(a @ 1)"]);
        assert_eq!(String::from_utf8_lossy(&out.stdout), "SELECT 1 AS a\n");

        // --assert / --if-errors removed — loud clap refusal, never a
        // flag that promises a check and runs none.
        for flag in ["--assert", "--if-errors"] {
            let out = run(&["query", flag, "x", "_(a @ 1)"]);
            assert!(!out.status.success(), "{flag} should be refused");
            assert!(
                String::from_utf8_lossy(&out.stderr).contains("unexpected argument"),
                "{flag} should be a clap unknown-argument error"
            );
        }

        // --dialect refuses bogus values eagerly (clap usage error,
        // rc=2); the postgresql alias is accepted; a bogus DQL_DIALECT env
        // refuses at startup (rc=1) instead of warning-and-ignoring.
        let out = run(&["query", "--dialect", "oracle", "_(a @ 1)"]);
        assert_eq!(
            out.status.code(),
            Some(2),
            "bogus --dialect must be a usage error"
        );
        let out = run(&[
            "query",
            "--dialect",
            "postgresql",
            "--to",
            "sql",
            "-f",
            "raw",
            "_(a @ 1)",
        ]);
        assert!(out.status.success(), "postgresql alias must be accepted");
        let out = std::process::Command::new(&cli_path)
            .env("DQL_DIALECT", "oracle")
            .args(["query", "_(a @ 1)"])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(1), "bogus DQL_DIALECT must refuse");
        assert!(String::from_utf8_lossy(&out.stderr).contains("unknown DQL_DIALECT"));

        // "cannot determine" is loud and the CI gate cannot bless
        // it (rc=2, distinct from rc=1 needs-formatting and rc=0
        // verified-formatted). Corpus coverage is total, so no fixture
        // construct pass-throughs; unparseable input exercises
        // the same contract in both modes.
        let out = run(&["format", "users(("]);
        assert_eq!(
            out.status.code(),
            Some(2),
            "parse error must exit 2 in plain mode"
        );
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("does not parse"),
            "pass-through must warn on stderr"
        );
        assert_eq!(
            String::from_utf8_lossy(&out.stdout),
            "users((",
            "unparseable input passes through unchanged"
        );
        let out = run(&["format", "--fail-if-not-formatted", "users(("]);
        assert_eq!(
            out.status.code(),
            Some(2),
            "gate must exit 2 on parse error"
        );
        let out = run(&["format", "--fail-if-not-formatted", "a(*),b(*),a.id=b.id"]);
        assert_eq!(out.status.code(), Some(1), "unformatted input exits 1");
        let out = run(&[
            "format",
            "--fail-if-not-formatted",
            "a(*), b(*), a.id = b.id",
        ]);
        assert_eq!(out.status.code(), Some(0), "formatted input exits 0");
    }

    /// The panic-exit, `--file -`, and completions-generate trio.
    #[test]
    fn test_cli_surface_recommendations_trio() {
        let cli_path = get_cli_path();

        // A panic exits 1 with a structured internal/panic record —
        // never rc=101 with a raw backtrace dump.
        let out = std::process::Command::new(&cli_path)
            .env("DQL_TEST_PANIC", "1")
            .env_remove("RUST_BACKTRACE")
            .arg("version")
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(1), "panic must exit 1, not 101");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("delightql-error://internal/panic"),
            "panic must emit the structured record: {stderr}"
        );
        assert!(
            !stderr.contains("thread 'main' panicked"),
            "raw panic output must not reach the user without RUST_BACKTRACE"
        );

        // --file - reads stdin.
        use std::io::Write as _;
        let mut child = std::process::Command::new(&cli_path)
            .args(["query", "--file", "-"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(b"_(a @ 42)").unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success(), "--file - must read stdin");
        assert!(String::from_utf8_lossy(&out.stdout).contains("42"));

        // completions generate for every supported shell.
        for shell in ["bash", "zsh", "fish", "elvish", "powershell"] {
            let out = std::process::Command::new(&cli_path)
                .args(["completions", shell])
                .output()
                .unwrap();
            assert!(out.status.success(), "completions {shell} failed");
            assert!(!out.stdout.is_empty(), "completions {shell} empty");
        }

        // Help hygiene: dead flags removed — loud clap refusal, never
        // a placebo. --quiet stays (REPL-only, documented).
        for flag in ["--strict", "--verbose", "--inline-ctes"] {
            let out = std::process::Command::new(&cli_path)
                .args(["query", flag, "_(a @ 1)"])
                .output()
                .unwrap();
            assert!(!out.status.success(), "{flag} should be refused");
        }

        // NO_COLOR suppresses auto-detected color (trivially true
        // when piped, but pins that the variable is at least consulted
        // without erroring).
        let out = std::process::Command::new(&cli_path)
            .env("NO_COLOR", "1")
            .args(["format", "a(*),b(*)"])
            .output()
            .unwrap();
        assert!(out.status.success());

        // --error-format json emits an RS-framed JSON record with
        // the identity URI as a field (RFC 7464 with the default prefix).
        let out = std::process::Command::new(&cli_path)
            .args(["query", "--error-format", "json", "nope(*)"])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(1));
        let stderr = String::from_utf8_lossy(&out.stderr);
        let line = stderr.lines().next().unwrap().trim_start_matches('\x1e');
        let rec: serde_json::Value =
            serde_json::from_str(line).expect("error record must be valid JSON");
        assert!(rec["uri"]
            .as_str()
            .unwrap()
            .starts_with("delightql-error://"));
        assert!(rec["message"].as_str().unwrap().contains("nope"));

        // A global flag the subcommand cannot consume refuses loudly;
        // both argument orders keep working where it IS consumed.
        let out = std::process::Command::new(&cli_path)
            .args(["version", "--db", "x.db"])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(1), "--db on version must refuse");
        assert!(String::from_utf8_lossy(&out.stderr).contains("means nothing"));
        let out = std::process::Command::new(&cli_path)
            .args(["--db", ":memory: no", "version"])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(1), "pre-subcommand --db too");
    }

    /// `cli::surface` is derived from the live clap tree, and the audit's
    /// drift-between-channels findings become a
    /// standing assertion — every option the binary accepts is
    /// documented in its man page. (The audit found help/man/binary
    /// drift BY HAND; this is that check, mechanized.)
    #[test]
    fn test_ring1_options_are_documented_in_man_pages() {
        let cli_path = get_cli_path();
        let out = std::process::Command::new(&cli_path)
            .args([
                "query",
                "-n",
                "-f",
                "jsonl",
                "cli::surface.option(*) |> (command, long)",
            ])
            .output()
            .unwrap();
        assert!(out.status.success());
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let man_dir = manifest.join("../../assets/man/man1");
        let page_for = |command: &str| -> &'static str {
            match command {
                "query" => "dql-query.1",
                "format" => "dql-format.1",
                "server" => "dql-server.1",
                "version" => "dql-version.1",
                "explain" => "dql-explain.1",
                "jstruct" | "csvstruct" | "filemunge" | "tools" => "dql-tools.1",
                "list" | "install" | "verify" | "target" => "dql-target.1",
                "editor" | "export-artifacts" => "dql-editor.1",
                // root globals + completions live in dql(1)
                _ => "dql.1",
            }
        };
        let mut checked = 0;
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            let rec: serde_json::Value = serde_json::from_str(line).unwrap();
            let (command, long) = (
                rec["command"].as_str().unwrap().to_string(),
                rec["long"].as_str().unwrap().to_string(),
            );
            let page = man_dir.join(page_for(&command));
            let troff = std::fs::read_to_string(&page)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", page.display()));
            // troff escapes dashes (\-\-file); strip escapes to search.
            let plain = troff.replace('\\', "");
            assert!(
                plain.contains(&long),
                "{long} (dql {command}) is accepted by the binary but not \
                 documented in {} — the drift class the surface audit \
                 found by hand, caught by assertion",
                page.display()
            );
            checked += 1;
        }
        assert!(checked >= 40, "suspiciously few option rows: {checked}");
    }

    /// The book pipeline, pinned WITHOUT any authored expectations
    /// (content is the author's — the test suite guarantees nothing
    /// about its form or words). Every expected
    /// value is derived from the bundle at runtime; what is asserted is
    /// that the pipeline's STAGES AGREE: the listing agrees with
    /// book_meta, the emission head agrees with frontmatter, each spine
    /// placement's shift is applied to that atom's own stored first line,
    /// the exported images agree with the image relation, and the two
    /// book sources (embedded, --db) agree byte for byte. Rename or
    /// restructure content freely: only machinery breaks fire this test.
    #[test]
    fn test_book_pipeline_stages_agree() {
        let cli_path = get_cli_path();
        let bundled = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../assets/bundled/books.sqlite");
        let query = |dql: &str| -> Vec<Vec<String>> {
            let out = std::process::Command::new(&cli_path)
                .args(["query", "--db"])
                .arg(&bundled)
                .arg(dql)
                .output()
                .unwrap();
            assert!(out.status.success(), "bundle query failed: {dql}");
            String::from_utf8_lossy(&out.stdout)
                .lines()
                .skip(1) // header row
                .map(|line| line.split('\t').map(str::to_string).collect())
                .collect()
        };
        let unhex = |hex: &str| -> String {
            let bytes = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect::<Vec<_>>();
            String::from_utf8(bytes).unwrap()
        };
        let marked = |line: &str| -> bool {
            let trimmed = line.trim_end();
            trimmed.ends_with('}')
                && trimmed.rfind('{').is_some_and(|open| {
                    trimmed[open + 1..trimmed.len() - 1]
                        .split_whitespace()
                        .any(|token| token == ".dqlh")
                })
        };

        // Agreement 1: bare `dql book` lists exactly book_meta's books.
        let books: Vec<String> = query("book_meta(*) |> #(book_name) |> (book_name)")
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        assert!(!books.is_empty());
        let listed = std::process::Command::new(&cli_path)
            .args(["book"])
            .output()
            .unwrap();
        assert!(listed.status.success());
        assert_eq!(
            String::from_utf8_lossy(&listed.stdout)
                .lines()
                .map(str::to_string)
                .collect::<Vec<_>>(),
            books
        );

        let flagship = &books[0];
        let rendered = std::process::Command::new(&cli_path)
            .args(["book", flagship])
            .output()
            .unwrap();
        assert!(rendered.status.success());
        let markdown = String::from_utf8_lossy(&rendered.stdout).to_string();
        assert!(!markdown.trim().is_empty());

        // Agreement 2: the emission leads with book_meta.frontmatter.
        let front = &query(&format!(
            "book_meta(*), book_name = \"{flagship}\" |> (hex:(frontmatter))"
        ))[0][0];
        if front != "NULL" && !front.is_empty() {
            assert!(markdown.starts_with(unhex(front).trim_end_matches('\n')));
        }

        // Agreement 3: for every placement whose stored content begins
        // with an ATX heading, the emission contains that same line with
        // the placement's shift applied (marked headings shift; unmarked
        // pass through verbatim). Derived from the bundle, never authored
        // into the test.
        let placements = query(&format!(
            "book(*), base_content(*.(slug)), book_name = \"{flagship}\" \
             |> #(ordinal) |> (heading_shift, hex:(substr:(content, 1, 400)))"
        ));
        assert!(!placements.is_empty());
        let mut sampled = 0;
        for placement in &placements {
            let shift: usize = placement[0].parse().unwrap();
            let head = unhex(&placement[1]);
            let first = head.lines().next().unwrap_or("");
            let hashes = first.chars().take_while(|c| *c == '#').count();
            if hashes == 0 || !first[hashes..].starts_with(' ') {
                continue; // not a heading-rooted atom: nothing to agree on
            }
            let expected = if marked(first) {
                format!("{}{}", "#".repeat(hashes + shift), &first[hashes..])
            } else {
                first.to_string()
            };
            assert!(
                markdown.contains(&expected),
                "book {flagship}: emission lacks {expected:?} (shift {shift})"
            );
            sampled += 1;
        }
        assert!(sampled > 0, "book {flagship}: nothing sampled");

        // Agreement 4: --export-images materializes exactly the image
        // relation, byte-faithful (spot-checked against the relation's
        // own digest length claim via file existence + count).
        let dir = std::env::temp_dir().join(format!("dql-book-images-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let exported = std::process::Command::new(&cli_path)
            .args(["book", flagship, "--export-images"])
            .arg(&dir)
            .output()
            .unwrap();
        assert!(exported.status.success());
        let image_names: Vec<String> = query("image(*) |> #(name) |> (name)")
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        for name in &image_names {
            assert!(dir.join(name).is_file(), "export missing {name}");
        }
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), image_names.len());
        std::fs::remove_dir_all(&dir).unwrap();

        // Refusal: exporting needs a book context.
        let refused_export = std::process::Command::new(&cli_path)
            .args(["book", "--export-images"])
            .output()
            .unwrap();
        assert!(!refused_export.status.success());

        // Agreement 5: the same bundle from disk is the same book
        // (BOOK-NEXT-GEN section 8) — embedded and --db byte-identical.
        let from_db = std::process::Command::new(&cli_path)
            .args(["book", flagship, "--db"])
            .arg(&bundled)
            .output()
            .unwrap();
        assert!(from_db.status.success());
        assert_eq!(from_db.stdout, rendered.stdout);

        let missing_db = std::process::Command::new(&cli_path)
            .args(["book", flagship, "--db", "/nonexistent-book.sqlite"])
            .output()
            .unwrap();
        assert!(!missing_db.status.success());
        assert!(String::from_utf8_lossy(&missing_db.stderr).contains("not found"));

        let refused = std::process::Command::new(&cli_path)
            .args(["book", "x\" |> nope(*)"])
            .output()
            .unwrap();
        assert_eq!(refused.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&refused.stderr).contains("invalid book name"));
    }

    #[test]
    fn test_cli_and_engine_namespaces_have_single_owners() {
        let cli_path = get_cli_path();
        let run = |query: &str| {
            std::process::Command::new(&cli_path)
                .args(["query", "-n", query])
                .output()
                .unwrap()
        };

        assert!(run("cli::surface.command(*) |> (name)").status.success());
        assert!(run("cli::(*)").status.success());
        assert!(run("sys::identifiers.identifier(*) |> (kind)")
            .status
            .success());
        assert!(!run("cli::book.book(*)").status.success());
        assert!(!run("cli::man.man_page(*)").status.success());
        assert!(!run("sys::help.identifier(*)").status.success());
    }

    /// The CLI-owned man database contains burned relations and
    /// `dql man` is their projection — grammar (hyphen-join, dql-
    /// prefix inference), --dump, and the rendering chain's last rung.
    #[test]
    fn test_dql_man_projects_the_man_page_table() {
        let cli_path = get_cli_path();

        // Short form + prefix inference: `dql man target` → dql-target(1).
        // Piped stdout = the plumbing face: plain derived from burned troff,
        // never man/groff (those own the TTY face, untestable here).
        let out = std::process::Command::new(&cli_path)
            .args(["man", "target"])
            .output()
            .unwrap();
        assert!(out.status.success());
        assert!(String::from_utf8_lossy(&out.stdout).contains("DQL-TARGET(1)"));

        // Mirror form: `dql man dql target` reaches the same page.
        let out = std::process::Command::new(&cli_path)
            .args(["man", "dql", "target"])
            .output()
            .unwrap();
        assert!(out.status.success());
        assert!(String::from_utf8_lossy(&out.stdout).contains("DQL-TARGET(1)"));

        // The pipe face is BYTE-EQUAL to the scrubber projection of the
        // burned troff column —
        // platform-independent by construction (a macOS build and a
        // Linux build pipe identical bytes, whatever man is installed).
        let piped = std::process::Command::new(&cli_path)
            .args(["man", "version"])
            .output()
            .unwrap();
        let plain =
            delightql_cli::man_scrub::scrub(include_str!("../../../assets/man/man1/dql-version.1"))
                .unwrap();
        assert!(piped.status.success());
        assert_eq!(
            piped.stdout,
            plain.into_bytes(),
            "piped dql man must serve exactly the scrubbed burned troff"
        );

        // A miss teaches: lists pages and hands over the apropos query.
        let out = std::process::Command::new(&cli_path)
            .args(["man", "nosuchpage"])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(1));
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("dql-query(1)"));
        assert!(stderr.contains("cli::man.man_page"));

        // --dump writes every embedded page (release staging).
        let dir = tempfile::tempdir().unwrap();
        let out = std::process::Command::new(&cli_path)
            .args(["man", "--dump"])
            .arg(dir.path())
            .output()
            .unwrap();
        assert!(out.status.success());
        let dumped = std::fs::read_dir(dir.path()).unwrap().count();
        assert!(dumped >= 8, "expected all embedded pages, got {dumped}");
    }

    /// `dql help <cmd>` is the same projection as `dql man <cmd>` — one
    /// mechanism, so the clap essay and the man page can never drift
    /// apart. Bare `dql help` keeps the usage summary.
    #[test]
    fn test_dql_help_is_the_man_page() {
        let cli_path = get_cli_path();

        // `dql help query` serves dql-query(1), not a clap essay.
        // Piped stdout = the derived plain projection, so the assertion is
        // environment-independent by construction.
        let out = std::process::Command::new(&cli_path)
            .args(["help", "query"])
            .output()
            .unwrap();
        assert!(out.status.success());
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains("DQL-QUERY(1)"), "expected the man page");
        assert!(
            !stdout.contains("Usage: dql query"),
            "clap essay leaked — help must project the man page"
        );

        // Bare `dql help` = the usage summary (same as --help).
        let out = std::process::Command::new(&cli_path)
            .args(["help"])
            .output()
            .unwrap();
        assert!(out.status.success());
        assert!(String::from_utf8_lossy(&out.stdout).contains("Usage:"));

        // A miss teaches, exactly like `dql man` (shared code path).
        let out = std::process::Command::new(&cli_path)
            .args(["help", "nosuchcommand"])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&out.stderr).contains("cli::man.man_page"));
    }

    /// The `delightql-bytes://` locator contract.
    /// The embedded book/man images are BOUND by open_handle and mounted by
    /// locator — attach-class (joinable), read-only, closed-namespace (no
    /// ambient authority), zero temp files.
    #[test]
    fn test_delightql_bytes_locator_contract() {
        let cli_path = get_cli_path();
        // `-f raw` only where a byte-faithful single column is asserted:
        // raw refuses multi-column receipts (mount!/unmount! ship one).
        let run_with = |program: &str, extra: &[&str]| {
            let mut args = vec!["query", "--sequential"];
            args.extend_from_slice(extra);
            let mut child = std::process::Command::new(&cli_path)
                .args(&args)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            use std::io::Write;
            child
                .stdin
                .take()
                .unwrap()
                .write_all(program.as_bytes())
                .unwrap();
            child.wait_with_output().unwrap()
        };
        let run_seq = |program: &str| run_with(program, &[]);

        // Closed namespace: an unbound name refuses and TEACHES — the sorted,
        // deliberately non-secret binding inventory.
        let out = run_seq("mount!(\"delightql-bytes://nosuch\", \"x\")(*)\n");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("no byte binding named 'nosuch'")
                && stderr.contains("book, editor, man, surface"),
            "miss must list the bound names, got: {stderr}"
        );

        // Attach-class: the mounted image is joinable with a main-side
        // relation in one query (the property the design pins).
        let out = run_with(
            "mount!(\"delightql-bytes://man\", \"m\")(*)\n\
             m.man_page(*), _(n @ \"dql-query\"), name = n ~> count:(*) as c |> (c)\n",
            &["-f", "raw"],
        );
        assert!(out.status.success());
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "1");

        // Full-citizen namespace: the `ns::(*)` listing construct resolves
        // for a bytes mount exactly as for a file mount (the tempting
        // regression: skipping register_mounted_catalog_wrappers makes
        // `m::(*)` 'Table not found').
        let out = run_seq("mount!(\"delightql-bytes://man\", \"m\")(*)\nm::(*)\n");
        assert!(out.status.success());
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            stdout.contains("man_page") && stdout.contains("bundle_meta"),
            "m::(*) must list the mounted namespace's entities, got: {stdout}"
        );

        // Read-only: DML against the mounted image refuses at SQLite's
        // readonly enforcement.
        let out = run_seq(
            "mount!(\"delightql-bytes://man\", \"m\")(*)\n\
             _(name, section, troff, content_digest @ \"x\", 9, \"t\", \"d\") \
             |> insert!(m.man_page(*))(*)\n",
        );
        assert!(String::from_utf8_lossy(&out.stderr).contains("readonly"));

        // Lifecycle: refresh! refuses (immutable image); mount_new! refuses
        // (attach-only locator); unmount! detaches cleanly.
        let out = run_seq("mount!(\"delightql-bytes://man\", \"m\")(*)\nrefresh!(\"m\")(*)\n");
        assert!(String::from_utf8_lossy(&out.stderr).contains("immutable"));
        let out = run_seq("mount_new!(\"delightql-bytes://book\", \"x\")(*)\n");
        assert!(!out.status.success(), "mount_new! must refuse a locator");
        let out = run_seq("mount!(\"delightql-bytes://man\", \"m\")(*)\nunmount!(\"m\")(*)\n");
        assert!(
            out.status.success(),
            "unmount! of a bytes mount must succeed"
        );

        // Zero temp files: dql book / dql man materialize nothing on disk.
        let tmp = std::env::temp_dir();
        let count_tempfiles = || {
            std::fs::read_dir(&tmp)
                .unwrap()
                .flatten()
                .filter(|e| {
                    let n = e.file_name().to_string_lossy().to_string();
                    n.starts_with("dql-book-") || n.starts_with("dql-man-")
                })
                .count()
        };
        let before = count_tempfiles();
        let out = std::process::Command::new(&cli_path)
            .args(["book", "reference"])
            .output()
            .unwrap();
        assert!(out.status.success());
        let out = std::process::Command::new(&cli_path)
            .args(["man", "version"])
            .output()
            .unwrap();
        assert!(out.status.success());
        assert_eq!(
            count_tempfiles(),
            before,
            "book/man must not materialize temp database files"
        );
    }

    /// `main`'s physical attachment identity is stored
    /// separately from its qualification policy (`cartridge.source_ns` is
    /// NULL for main by design, so it cannot carry the ATTACH alias).
    /// unmount!("main") DETACHes and EMPTIES the bootstrap fixture rather
    /// than destroying its wiring; remount restores full service —
    /// including unqualified reads — and refresh introspects the attached
    /// database, not SQLite's hub.
    #[test]
    fn test_main_mount_lifecycle_keeps_identity_and_wiring() {
        let cli_path = get_cli_path();
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("mainlc.sqlite");
        {
            let conn = rusqlite::Connection::open(&db).unwrap();
            conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (7);")
                .unwrap();
        }
        let db = db.to_string_lossy().to_string();
        let run = |program: &str| {
            let mut child = std::process::Command::new(&cli_path)
                .args(["query", "--db", &db, "--sequential", "-f", "raw"])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            use std::io::Write;
            child
                .stdin
                .take()
                .unwrap()
                .write_all(program.as_bytes())
                .unwrap();
            child.wait_with_output().unwrap()
        };

        // unmount → remount → remount again → UNQUALIFIED read still works.
        // A schema alias left attached because source_ns is NULL for main
        // answers '_imported_N is already in use' on the first remount.
        let out = run(&format!(
            "unmount!(\"main\")(*)\nmount!(\"{db}\", \"main\")(*)\nunmount!(\"main\")(*)\nmount!(\"{db}\", \"main\")(*)\nt(*) |> (x)\n"
        ));
        assert!(
            out.status.success(),
            "main unmount/remount cycles must keep identity and wiring: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "7");

        // refresh!("main") introspects the ATTACHED database. Resolving the
        // namespace name to the hub instead introspects the hub and loses
        // the catalog.
        let out = run("refresh!(\"main\")(*)\nt(*) |> (x)\n");
        assert!(
            out.status.success(),
            "refresh of main must re-introspect the attached db: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "7");
    }

    /// Mount identity is
    /// AUTHORITATIVE — a valid-but-EMPTY database mounts idempotently,
    /// conflicts correctly, and unmounts cleanly, even though it activates
    /// zero entities. Deriving identity from activated_entity joins
    /// instead leaves an empty image with no identity: re-mounts would
    /// attach duplicate aliases and unmount could not find the alias to
    /// DETACH.
    #[test]
    fn test_mount_spine_empty_image_lifecycle() {
        let cli_path = get_cli_path();
        // A VALID SQLite database containing no tables at all (a 0-byte file
        // would be refused as invalid; this one has a real header).
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("empty.sqlite");
        {
            let conn = rusqlite::Connection::open(&db).unwrap();
            conn.execute_batch("CREATE TABLE t(x); DROP TABLE t;")
                .unwrap();
        }
        let db = db.to_string_lossy().to_string();

        let run_seq = |program: &str| {
            let mut child = std::process::Command::new(&cli_path)
                .args(["query", "--sequential"])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            use std::io::Write;
            child
                .stdin
                .take()
                .unwrap()
                .write_all(program.as_bytes())
                .unwrap();
            child.wait_with_output().unwrap()
        };

        // Idempotent re-mount, working namespace functor with ZERO entities,
        // authoritative refresh (re-introspects to zero entities rather than
        // "no cartridge"), clean unmount — one session.
        let out = run_seq(&format!(
            "mount!(\"{db}\", \"e\")(*)\nmount!(\"{db}\", \"e\")(*)\ne::(*)\nrefresh!(\"e\")(*)\nunmount!(\"e\")(*)\n"
        ));
        assert!(
            out.status.success(),
            "empty image must mount idempotently, answer e::(*), refresh, and unmount cleanly: {}",
            String::from_utf8_lossy(&out.stderr)
        );

        // Conflict detection still works without entities: a different
        // source over the same namespace refuses.
        let out = run_seq(&format!(
            "mount!(\"{db}\", \"e\")(*)\nmount!(\"delightql-bytes://man\", \"e\")(*)\n"
        ));
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("already exists"),
            "different source over an empty-image namespace must conflict: {stderr}"
        );

        // SIMULTANEOUS empty mounts of one source are LEGAL: the stored
        // link makes each namespace's cartridge distinguishable.
        // Unmounting one detaches only its own alias — the other
        // stays queryable — and the once-corrupting full sequence (mount
        // a+b, unmount both, mount c+d) runs clean with no leaked alias.
        let out = run_seq(&format!(
            "mount!(\"{db}\", \"a\")(*)\nmount!(\"{db}\", \"b\")(*)\nunmount!(\"a\")(*)\nb::(*)\nunmount!(\"b\")(*)\nmount!(\"{db}\", \"c\")(*)\nmount!(\"{db}\", \"d\")(*)\nunmount!(\"c\")(*)\nunmount!(\"d\")(*)\n"
        ));
        assert!(
            out.status.success(),
            "simultaneous empty mounts must be legal and leak no aliases: {}",
            String::from_utf8_lossy(&out.stderr)
        );

        // Refreshing an EMPTY image must not duplicate its cartridge: a
        // duplicate makes unmount sweep both rows but detach
        // one alias, poisoning later mounts of the same source. Repeated
        // refresh + unmount + remount stays clean.
        let out = run_seq(&format!(
            "mount!(\"{db}\", \"e\")(*)\nrefresh!(\"e\")(*)\nrefresh!(\"e\")(*)\nunmount!(\"e\")(*)\nmount!(\"{db}\", \"f\")(*)\nunmount!(\"f\")(*)\n"
        ));
        assert!(
            out.status.success(),
            "refreshing an empty image must not duplicate its cartridge: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Help must not teach flags a
    /// subcommand refuses (each global's one-liner names its
    /// consumers), and --to's compile failures must carry the full
    /// message, not a URI plus advice to re-run without --to.
    #[test]
    fn test_alpha_ux_worries_1_and_4() {
        let cli_path = get_cli_path();

        // tools --help shows propagated globals (clap has no
        // per-subcommand hiding), so the flag text itself must teach
        // where each one works.
        let out = std::process::Command::new(&cli_path)
            .args(["tools", "--help"])
            .output()
            .unwrap();
        let help = String::from_utf8_lossy(&out.stdout);
        assert!(help.contains("query, server, book; others refuse"));
        assert!(help.contains("(query only)"));
        assert!(help.contains("(query, tools, server)"));

        // A --to compile failure emits the SAME record shape as
        // normal execution — URI and full prose, no withholding.
        let out = std::process::Command::new(&cli_path)
            .args(["query", "--to", "sql", "_(1,2) |> (id, nope)"])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(1));
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("delightql-error://semantic/resolution/column"));
        assert!(
            stderr.contains("Column not found"),
            "the full message must ride with the URI: {stderr}"
        );
        assert!(
            !stderr.contains("without --to"),
            "the re-run-yourself advice must be gone"
        );
    }

    /// Numeric values in anonymous tables,
    /// aggregates, and computed columns must emit as JSON numbers.
    /// sqlite declares no decl_type for expression columns, so the relay
    /// elects the engine's own storage class from the column's FIRST
    /// NON-NULL value (a bounded peek) — a declaration by the
    /// engine, not a parsing heuristic; the round-trip guard still demotes
    /// mismatched cells per-value.
    #[test]
    fn test_alpha_ux_worry_3_expression_typing() {
        let cli_path = get_cli_path();
        let json = |query: &str| {
            let out = std::process::Command::new(&cli_path)
                .args(["query", "-f", "json", query])
                .output()
                .unwrap();
            assert!(out.status.success());
            serde_json::from_slice::<serde_json::Value>(&out.stdout).unwrap()
        };

        // The reviewer's exact case: a numeric literal stays a number.
        let v = json("_(name, age @ \"Ada\", 36)");
        assert_eq!(v[0]["age"], serde_json::json!(36));
        assert_eq!(v[0]["name"], serde_json::json!("Ada"));

        // Aggregates carry the same requirement over REAL tables too. The
        // column is ALIASED: an unaliased one carries an invented name, and
        // invented names are output only and deliberately unstable, so
        // keying a test on one would assert the opposite of the ruling.
        let v = json("_(1,2;3,4) ~> count:(*) as n");
        assert_eq!(v[0]["n"], serde_json::json!(2));

        // Text that merely looks numeric is still governed by its
        // declaration: TEXT storage class → string.
        let v = json("_(pad @ \"007\")");
        assert_eq!(v[0]["pad"], serde_json::json!("007"));

        // A NULL-leading column does not type off row 0: it elects INTEGER
        // from its first non-NULL value; the leading NULL stays null, the 5
        // emits as a number. Typing off row 0 instead would make this whole
        // column stringly, rendering `5` as "5".
        let v = json("_(x @ null; 5)");
        assert_eq!(v[0]["x"], serde_json::Value::Null);
        assert_eq!(
            v[1]["x"],
            serde_json::json!(5),
            "null-leading elects INTEGER"
        );

        // Row-order independence: reversing the rows yields identical JSON
        // types (only the values swap) — `5; null` and `null; 5` must not
        // diverge into different JSON types for the same data.
        let v = json("_(x @ 5; null)");
        assert_eq!(v[0]["x"], serde_json::json!(5));
        assert_eq!(v[1]["x"], serde_json::Value::Null);

        // Genuinely mixed column: election happens from row 0 (INTEGER 1),
        // so the numeric cells (1, 2) emit as numbers and the interloping
        // "abc" — which does not round-trip against INTEGER — is demoted to
        // a string per-value by the round-trip guard. Mixed-type columns
        // remain order-dependent by design (declare or cast if a consumer
        // depends on it); this pins what the ruled mechanism yields.
        let v = json("_(x @ 1; \"abc\"; 2)");
        assert_eq!(v[0]["x"], serde_json::json!(1));
        assert_eq!(v[1]["x"], serde_json::json!("abc"));
        assert_eq!(v[2]["x"], serde_json::json!(2));
    }

    /// Bare `dql` is sugar for `dql query` with no arguments — one
    /// rule, one code path. The tempting regression: bare dql with
    /// piped stdin printing the REPL banner, silently DISCARDing
    /// the piped input, and exiting 0 (`dql < queries.dql` reporting
    /// success with nothing executed).
    #[test]
    fn test_bare_dql_is_sugar_for_query() {
        use std::io::Write;
        let cli_path = get_cli_path();
        // Byte-equality is the claim, so the two runs must be comparable
        // byte for byte: a heading nobody authored is DRAWN per compilation,
        // and two processes are two draws. The canonical policy renders the
        // same invented names as `<mint:N>`, which is what lets this test go
        // on asserting the road rather than a spelling.
        let run_piped = |args: &[&str], stdin: &str| {
            let mut child = std::process::Command::new(&cli_path)
                .args(args)
                .env("DQL_NAME_POLICY", "canonical")
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(stdin.as_bytes())
                .unwrap();
            child.wait_with_output().unwrap()
        };

        // Piped source executes — byte-equal to `dql query`.
        let bare = run_piped(&[], "_(1,2;3,4)");
        let query = run_piped(&["query"], "_(1,2;3,4)");
        assert!(bare.status.success());
        assert_eq!(bare.stdout, query.stdout, "bare dql must BE dql query");
        assert!(String::from_utf8_lossy(&bare.stdout).contains('3'));
        assert!(
            !String::from_utf8_lossy(&bare.stdout).contains("REPL"),
            "no banner on a pipe"
        );

        // Empty piped stdin refuses loudly (matching dql query), not
        // banner-then-exit-0.
        let bare = run_piped(&[], "");
        assert!(!bare.status.success());
        assert!(String::from_utf8_lossy(&bare.stderr).contains("No input"));

        // --make-new-db-if-missing is global (--db's companion knob),
        // so the remedy the missing-db refusal teaches is reachable at
        // the bare door too.
        let dir = tempfile::tempdir().unwrap();
        let fresh = dir.path().join("fresh.db");
        let bare = run_piped(
            &["--db", fresh.to_str().unwrap(), "--make-new-db-if-missing"],
            "_(1)",
        );
        assert!(
            bare.status.success(),
            "bare door must accept the flag the refusal teaches: {}",
            String::from_utf8_lossy(&bare.stderr)
        );
        assert!(fresh.exists(), "database file must be created");

        // But alone it configures nothing (requires --db), and non-query
        // subcommands refuse it (the global-option hygiene rule).
        let out = std::process::Command::new(&cli_path)
            .args(["--make-new-db-if-missing"])
            .output()
            .unwrap();
        assert_eq!(
            out.status.code(),
            Some(2),
            "flag without --db is a usage error"
        );
        let out = std::process::Command::new(&cli_path)
            .args(["version", "--db", "x.db", "--make-new-db-if-missing"])
            .output()
            .unwrap();
        assert!(!out.status.success());
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("means nothing to 'dql version'"),
            "R6 must refuse the unconsumed global"
        );
    }

    /// The engine identifier registry is a burned
    /// relation, and `dql explain` is a projection of it — one source,
    /// two faces, pinned to agree.
    #[test]
    fn test_sys_identifiers_is_the_registry() {
        let cli_path = get_cli_path();
        let run = |args: &[&str]| {
            std::process::Command::new(&cli_path)
                .args(args)
                .output()
                .unwrap()
        };

        // The table is user-queryable and holds the registry rows.
        let out = run(&[
            "query",
            "-n",
            "sys::identifiers.identifier(*), hierarchy = \"internal/panic\" |> (summary)",
        ]);
        assert!(out.status.success());
        let table_summary = String::from_utf8_lossy(&out.stdout).trim().to_string();
        assert!(
            table_summary.contains("dql itself crashed"),
            "table row missing: {table_summary}"
        );

        // explain projects the same row — the two faces cannot disagree.
        let out = run(&["explain", "internal/panic"]);
        assert!(out.status.success());
        assert!(
            String::from_utf8_lossy(&out.stdout).contains(&table_summary),
            "explain and the table disagree"
        );

        // Family listing still works (segment-prefix over rows).
        let out = run(&["explain", "delightql-error://semantic/recursion"]);
        assert!(out.status.success());
        assert!(String::from_utf8_lossy(&out.stdout).contains("Registered under this family"));
    }

    /// A machine format that lies about types is a contradiction: -f json
    /// and -f jsonl emit numbers for numerically-DECLARED columns whose
    /// text round-trips, null for NULL, strings for everything else,
    /// columns in relation order.
    #[test]
    fn test_json_types_do_not_lie() {
        let cli_path = get_cli_path();
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("types.db");
        {
            let conn = rusqlite::Connection::open(&db).unwrap();
            conn.execute_batch(
                "CREATE TABLE t(id INTEGER, name TEXT, score REAL, pad TEXT);
                 INSERT INTO t VALUES (1,'Ada',3.5,'007'),(2,NULL,NULL,'42');",
            )
            .unwrap();
        }
        let db = db.to_str().unwrap();

        let out = std::process::Command::new(&cli_path)
            .args(["query", "--db", db, "-f", "jsonl", "t(*)"])
            .output()
            .unwrap();
        assert!(out.status.success());
        let stdout = String::from_utf8_lossy(&out.stdout);
        // INTEGER unquoted; TEXT quoted even when numeric ('007' AND '42' —
        // declaration governs, not parseability); NULL is null; REAL
        // unquoted; relation column order preserved.
        assert_eq!(
            stdout,
            "{\"id\": 1, \"name\": \"Ada\", \"score\": 3.5, \"pad\": \"007\"}\n\
             {\"id\": 2, \"name\": null, \"score\": null, \"pad\": \"42\"}\n"
        );

        // -f json: same typing, one VALID array (the old path emitted
        // concatenated arrays past 100 rows — invalid JSON).
        let out = std::process::Command::new(&cli_path)
            .args(["query", "--db", db, "-f", "json", "t(*)"])
            .output()
            .unwrap();
        assert!(out.status.success());
        let parsed: serde_json::Value =
            serde_json::from_slice(&out.stdout).expect("-f json must emit valid JSON");
        assert_eq!(parsed[0]["id"], serde_json::json!(1));
        assert_eq!(parsed[1]["name"], serde_json::Value::Null);
        assert_eq!(parsed[1]["pad"], serde_json::json!("42"));
    }

    /// Raw is byte-faithful single-column extraction. Verbatim-ness is
    /// FROZEN at v0.1 — these assertions are that freeze.
    #[test]
    fn test_raw_is_byte_faithful_single_column() {
        let cli_path = get_cli_path();
        let run = |args: &[&str]| {
            std::process::Command::new(&cli_path)
                .args(args)
                .output()
                .unwrap()
        };

        // Multi-row single column: byte-stream concatenation, no
        // separators (a separator would corrupt binary), no trailing
        // newline.
        let out = run(&["query", "-f", "raw", "_(9;8)"]);
        assert!(out.status.success());
        assert_eq!(out.stdout, b"98", "raw must concatenate verbatim bytes");

        // Multi-column: refuse and teach, never "1John2Jane".
        let out = run(&["query", "-f", "raw", "_(1, \"hello\")"]);
        assert_eq!(out.status.code(), Some(1));
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("ONE column"),
            "multi-column raw must refuse with the teaching message"
        );

        // The compile-stage face keeps its trailing newline (text
        // artifact, not byte extraction — documented per-surface).
        let out = run(&["query", "--to", "sql", "-f", "raw", "_(a @ 1)"]);
        assert_eq!(String::from_utf8_lossy(&out.stdout), "SELECT 1 AS a\n");
    }
}
