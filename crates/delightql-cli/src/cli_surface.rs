// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The running CLI's live command surface, materialized as a CLI-owned
//! SQLite database and mounted at `cli::surface`.
//!
//! Commands and options are introspected from the LIVE clap tree —
//! they structurally cannot drift from what the binary accepts. Two
//! kinds of rows are enriched by hand, each from the same source its
//! parser uses (still one upstream):
//! - option VALUES for custom-parser flags (`-f` from
//!   `OutputFormat::all_formats()`, `--dialect` from the family list
//!   `is_known_dialect_family` accepts) — clap only exposes
//!   possible-values for ValueEnum args;
//! - class/grade: the declaration is a design ruling, not
//!   introspectable. This module IS the declaration-of-record for
//!   enumerable surfaces; a separate inventory table covers
//!   non-flag-shaped surfaces (RS framing, server socket line, …).
//!
//! Construction cadence is deliberately explicit: `connection::open_handle`
//! calls `attach` synchronously, so this database is built and mounted once
//! per CLI handle, before that handle is returned. The wrapper owns the
//! tempfile for the handle's whole lifetime; subsequent sessions and relays
//! reuse the mount. Commands that never open a DQL handle pay no surface cost.

use anyhow::Result;
use clap::CommandFactory;
use delightql_core::api::DqlHandle;
use rusqlite::params;

const SURFACE_APPLICATION_ID: i64 = 0x4451_4c53; // DQLS

#[derive(Default)]
struct Surface {
    commands: Vec<(String, Option<String>, Option<String>, String)>,
    options: Vec<SurfaceOption>,
    option_values: Vec<(
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
    )>,
    dot_commands: Vec<(String, String)>,
    envs: Vec<(String, String, Option<String>)>,
    exit_codes: Vec<(i64, String, String, Option<String>, Option<String>)>,
}

struct SurfaceOption {
    command: String,
    long: String,
    short: Option<String>,
    value_name: Option<String>,
    default_value: Option<String>,
    global: bool,
    repeatable: bool,
    summary: String,
}

fn build() -> Surface {
    let mut surface = Surface::default();
    let root = crate::args::CliArgs::command();

    walk_command(&root, None, &mut surface);
    enrich_option_values(&mut surface);
    seed_envs(&mut surface);
    seed_exit_codes(&mut surface);
    seed_dot_commands(&mut surface);
    surface
}

fn walk_command(cmd: &clap::Command, parent: Option<&str>, surface: &mut Surface) {
    let name = if parent.is_none() {
        "dql".to_string()
    } else {
        cmd.get_name().to_string()
    };
    let aliases: Vec<&str> = cmd.get_visible_aliases().collect();
    surface.commands.push((
        name.clone(),
        parent.map(|p| p.to_string()),
        if aliases.is_empty() {
            None
        } else {
            Some(aliases.join(","))
        },
        cmd.get_about().map(|s| s.to_string()).unwrap_or_default(),
    ));

    for arg in cmd.get_arguments() {
        // clap's built-ins document themselves.
        if matches!(arg.get_id().as_str(), "help" | "version") {
            continue;
        }
        let Some(long) = arg.get_long() else {
            continue; // positionals live in the command summary/man page
        };
        surface.options.push(SurfaceOption {
            command: name.clone(),
            long: format!("--{}", long),
            short: arg.get_short().map(|c| format!("-{}", c)),
            value_name: arg
                .get_value_names()
                .and_then(|v| v.first())
                .map(|s| s.to_string()),
            default_value: arg
                .get_default_values()
                .first()
                .map(|v| v.to_string_lossy().to_string()),
            global: arg.is_global_set(),
            repeatable: matches!(arg.get_action(), clap::ArgAction::Append),
            summary: arg.get_help().map(|s| s.to_string()).unwrap_or_default(),
        });
        // ValueEnum args expose their vocabulary; record it.
        for pv in arg.get_possible_values() {
            surface.option_values.push((
                name.clone(),
                format!("--{}", long),
                pv.get_name().to_string(),
                pv.get_help().map(|s| s.to_string()),
                None,
                None,
            ));
        }
    }

    for sub in cmd.get_subcommands() {
        if sub.get_name() == "help" {
            continue;
        }
        walk_command(sub, Some(&name), surface);
    }
}

/// Custom-parser flags (`-f`, `--dialect`, `--to` values' classes):
/// values from the same functions their parsers consult, classes per
/// the ratified porcelain declaration.
fn enrich_option_values(surface: &mut Surface) {
    // -f formats: table/box/list are porcelain (pretty at will); raw is
    // frozen plumbing (byte-preservation exit); csv/tsv/json/jsonl are
    // versioned plumbing (machine formats, additive-only changes).
    for fmt in crate::output_format::OutputFormat::all_formats() {
        let (class, grade) = match *fmt {
            "table" | "box" | "list" => (Some("porcelain"), None),
            "raw" => (Some("plumbing"), Some("frozen")),
            _ => (Some("plumbing"), Some("versioned")), // json, jsonl, csv, tsv
        };
        surface.option_values.push((
            "query".to_string(),
            "--format".to_string(),
            fmt.to_string(),
            None,
            class.map(String::from),
            grade.map(String::from),
        ));
    }
    // --dialect families (aliases included) — what parse_dialect accepts.
    for d in [
        "sqlite",
        "postgres",
        "postgresql",
        "mysql",
        "sqlserver",
        "duckdb",
    ] {
        debug_assert!(delightql_core::is_known_dialect_family(d));
        surface.option_values.push((
            "dql".to_string(),
            "--dialect".to_string(),
            d.to_string(),
            None,
            None,
            None,
        ));
    }
    // --to stages already introspected (ValueEnum); stamp the ruled
    // classes onto the hash/artifact stages.
    for v in surface.option_values.iter_mut() {
        if v.1 == "--to" {
            match v.2.as_str() {
                "hash" | "bhash" | "totalhash" | "fingerprint" => {
                    v.4 = Some("plumbing".to_string());
                    v.5 = Some("frozen".to_string());
                }
                "sql" | "ast-unresolved" | "ast-resolved" | "ast-refined" | "ast-sql" | "cst" => {
                    v.4 = Some("porcelain+semantic-warranty".to_string());
                }
                _ => {}
            }
        }
    }
}

fn seed_envs(surface: &mut Surface) {
    let envs: &[(&str, &str, Option<&str>)] = &[
        (
            "DQL_DIALECT",
            "Target SQL dialect override; unknown values refuse at startup",
            Some("--dialect"),
        ),
        (
            "DQL_FORMAT",
            "Default output format when -f is absent",
            Some("--format"),
        ),
        (
            "DQL_FATBOY_DIR",
            "Hard-pins the adapter binary directory (only this directory is searched)",
            None,
        ),
        (
            "DQL_NAME_POLICY",
            "How a name the compiler invented is spelled: 'poison' (default, \
             drawn fresh each compilation) or 'canonical' (<mint:N>, for a \
             contract lane); unknown values refuse",
            None,
        ),
        (
            "NO_COLOR",
            "Color auto-detection yields no color (no-color.org); explicit --color wins",
            Some("--color"),
        ),
    ];
    for (name, effect, flag) in envs {
        surface
            .envs
            .push((name.to_string(), effect.to_string(), flag.map(String::from)));
    }
}

/// The ratified exit-code policy (dql(1) EXIT STATUS): 0/1 + structured
/// stderr records everywhere; 2 usage; dedicated gate flags document
/// their own codes.
fn seed_exit_codes(surface: &mut Surface) {
    let codes: &[(i64, &str, &str)] = &[
        (0, "global", "Success"),
        (
            1,
            "global",
            "Error; a structured record is emitted on stderr (panics included: internal/panic)",
        ),
        (
            2,
            "usage",
            "Command-line usage error (unknown flag or invalid value)",
        ),
        (
            1,
            "format --fail-if-not-formatted",
            "Input is not formatted",
        ),
        (
            2,
            "format --fail-if-not-formatted",
            "Cannot verify: the formatter passed the input through",
        ),
    ];
    for (code, context, meaning) in codes {
        surface.exit_codes.push((
            *code,
            context.to_string(),
            meaning.to_string(),
            Some("plumbing".to_string()),
            Some("versioned".to_string()),
        ));
    }
}

#[cfg(feature = "repl")]
fn seed_dot_commands(surface: &mut Surface) {
    // Projection of the REPL's dot-command registry — the same table the
    // dispatcher is welded to (registry_and_dispatch_agree), so these
    // rows structurally cannot drift from what the REPL accepts. One row
    // per accepted SPELLING: aliases get their own row naming the
    // canonical form.
    for cmd in crate::repl::commands::DOT_COMMANDS {
        surface
            .dot_commands
            .push((cmd.name.to_string(), cmd.summary.to_string()));
        for alias in cmd.aliases {
            surface.dot_commands.push((
                alias.to_string(),
                format!("{} (alias of {})", cmd.summary, cmd.name),
            ));
        }
    }
}

#[cfg(not(feature = "repl"))]
fn seed_dot_commands(surface: &mut Surface) {
    // No REPL in this build → honestly empty rows, same contract as
    // headless HelpSurface tables.
    let _ = surface;
}

/// Build the live surface as a serialized in-memory SQLite image — no
/// tempfile, no disk. The image is bound as owned bytes and mounted via
/// `delightql-bytes://surface`, so the catalog records honest provenance
/// (`bytes`, the locator) instead of a `/tmp` path.
fn serialize_surface() -> Result<Vec<u8>> {
    let surface = build();
    let mut conn = rusqlite::Connection::open_in_memory()?;
    conn.execute_batch(&format!(
        "PRAGMA application_id = {SURFACE_APPLICATION_ID}; PRAGMA user_version = {};",
        crate::embedded_db::SCHEMA_VERSION
    ))?;
    (move |conn: &mut rusqlite::Connection| -> Result<()> {
            conn.execute_batch(
                "CREATE TABLE command (
                     name TEXT NOT NULL,
                     parent TEXT,
                     alias TEXT,
                     summary TEXT NOT NULL,
                     PRIMARY KEY (name, parent)
                 );
                 CREATE TABLE option (
                     command TEXT NOT NULL,
                     long TEXT NOT NULL,
                     short TEXT,
                     value_name TEXT,
                     default_value TEXT,
                     global INTEGER NOT NULL,
                     repeatable INTEGER NOT NULL,
                     summary TEXT NOT NULL,
                     PRIMARY KEY (command, long)
                 );
                 CREATE TABLE option_value (
                     command TEXT NOT NULL,
                     option TEXT NOT NULL,
                     value TEXT NOT NULL,
                     summary TEXT,
                     class TEXT,
                     grade TEXT,
                     PRIMARY KEY (command, option, value)
                 );
                 CREATE TABLE dot_command (
                     name TEXT PRIMARY KEY,
                     summary TEXT NOT NULL
                 );
                 CREATE TABLE env (
                     name TEXT PRIMARY KEY,
                     effect TEXT NOT NULL,
                     equivalent_flag TEXT
                 );
                 CREATE TABLE exit_code (
                     code INTEGER NOT NULL,
                     context TEXT NOT NULL,
                     meaning TEXT NOT NULL,
                     class TEXT,
                     grade TEXT,
                     PRIMARY KEY (code, context)
                 );",
            )?;
            let tx = conn.transaction()?;
            for (name, parent, alias, summary) in surface.commands {
                tx.execute(
                    "INSERT INTO command VALUES (?1, ?2, ?3, ?4)",
                    params![name, parent, alias, summary],
                )?;
            }
            for option in surface.options {
                tx.execute(
                    "INSERT INTO option VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        option.command,
                        option.long,
                        option.short,
                        option.value_name,
                        option.default_value,
                        option.global,
                        option.repeatable,
                        option.summary
                    ],
                )?;
            }
            for (command, option, value, summary, class, grade) in surface.option_values {
                tx.execute(
                    "INSERT INTO option_value VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![command, option, value, summary, class, grade],
                )?;
            }
            for (name, summary) in surface.dot_commands {
                tx.execute(
                    "INSERT INTO dot_command VALUES (?1, ?2)",
                    params![name, summary],
                )?;
            }
            for (name, effect, flag) in surface.envs {
                tx.execute(
                    "INSERT INTO env VALUES (?1, ?2, ?3)",
                    params![name, effect, flag],
                )?;
            }
            for (code, context, meaning, class, grade) in surface.exit_codes {
                tx.execute(
                    "INSERT INTO exit_code VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![code, context, meaning, class, grade],
                )?;
            }
            tx.commit()?;
            Ok(())
    })(&mut conn)?;
    let data = conn.serialize("main")?;
    Ok(data.to_vec())
}

pub fn attach(mut inner: Box<dyn DqlHandle>) -> Result<Box<dyn DqlHandle>> {
    let image = serialize_surface()?;
    inner
        .bind_owned_bytes("surface", image)
        .map_err(|e| anyhow::anyhow!(e))?;
    {
        let mut session = inner.session().map_err(|e| anyhow::anyhow!(e))?;
        crate::exec_ng::run_dql_query(
            "mount!(\"delightql-bytes://surface\", \"cli::surface\")(*)",
            &mut *session,
        )?;
    }
    // No wrapper handle: there is no tempfile to keep alive. The mounted
    // image is SQLite-owned memory on the session connection.
    Ok(inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    /// Measurement-only: timing is reported, never asserted. This isolates
    /// the live-Clap traversal plus in-memory SQLite creation, serialization, and
    /// seeding performed once by every `open_handle()` call.
    ///
    /// Run with:
    /// `cargo test -p delightql-cli cli_surface::tests::measure_materialization_cost -- --ignored --nocapture`
    #[test]
    #[ignore = "measurement-only benchmark; no timing threshold belongs in CI"]
    fn measure_materialization_cost() {
        const WARMUPS: usize = 5;
        const SAMPLES: usize = 50;

        for _ in 0..WARMUPS {
            let database = serialize_surface().expect("serialize cli::surface");
            std::hint::black_box(&database);
        }

        let mut samples = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let started = Instant::now();
            let database = serialize_surface().expect("serialize cli::surface");
            std::hint::black_box(&database);
            samples.push(started.elapsed());
        }
        samples.sort_unstable();

        let mean_ms = samples
            .iter()
            .map(|duration| duration.as_secs_f64() * 1_000.0)
            .sum::<f64>()
            / SAMPLES as f64;
        let median_ms = samples[SAMPLES / 2].as_secs_f64() * 1_000.0;
        let p95_index = (SAMPLES * 95).div_ceil(100) - 1;
        let p95_ms = samples[p95_index].as_secs_f64() * 1_000.0;
        let min_ms = samples[0].as_secs_f64() * 1_000.0;

        eprintln!(
            "cli::surface materialization: samples={SAMPLES} min={min_ms:.3}ms median={median_ms:.3}ms p95={p95_ms:.3}ms mean={mean_ms:.3}ms"
        );
    }
}
