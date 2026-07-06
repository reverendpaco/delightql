// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! sys::help ring 1 (SYS-HELP-DESIGN.md phase 2): describe this
//! binary's own surface as data, for core to seed into the
//! `sys::help.*` tables at session init.
//!
//! Commands and options are introspected from the LIVE clap tree —
//! they structurally cannot drift from what the binary accepts. Two
//! kinds of rows are enriched by hand, each from the same source its
//! parser uses (still one upstream):
//! - option VALUES for custom-parser flags (`-f` from
//!   `OutputFormat::all_formats()`, `--dialect` from the family list
//!   `is_known_dialect_family` accepts) — clap only exposes
//!   possible-values for ValueEnum args;
//! - class/grade (PORCELAIN-AND-PLUMBING.md): the declaration is a
//!   design ruling, not introspectable. This module IS the
//!   declaration-of-record for enumerable surfaces; the doc's
//!   inventory table remains for the rest.

use clap::CommandFactory;
use delightql_core::api::{HelpOption, HelpSurface};

pub fn build() -> HelpSurface {
    let mut surface = HelpSurface::default();
    let root = crate::args::CliArgs::command();

    walk_command(&root, None, &mut surface);
    enrich_option_values(&mut surface);
    seed_envs(&mut surface);
    seed_exit_codes(&mut surface);
    seed_dot_commands(&mut surface);
    seed_man_pages(&mut surface);
    surface
}

/// The shipped man pages, embedded at compile time (the CLI owns its
/// own documentation; core stays ignorant of any particular host's
/// pages). `plain` is scrubbed HERE, at surface-build time — one
/// upstream (the troff), in sync by construction; the closed-dialect
/// scrubber refuses unknown markup and the man_scrub tests keep every
/// page inside the dialect, so this expect cannot fire for shipped
/// pages.
fn seed_man_pages(surface: &mut HelpSurface) {
    const PAGES: &[(&str, i64, &str)] = &[
        ("dql", 1, include_str!("../../../man/man1/dql.1")),
        ("dql-query", 1, include_str!("../../../man/man1/dql-query.1")),
        ("dql-format", 1, include_str!("../../../man/man1/dql-format.1")),
        ("dql-tools", 1, include_str!("../../../man/man1/dql-tools.1")),
        ("dql-explain", 1, include_str!("../../../man/man1/dql-explain.1")),
        ("dql-target", 1, include_str!("../../../man/man1/dql-target.1")),
        ("dql-server", 1, include_str!("../../../man/man1/dql-server.1")),
        ("dql-version", 1, include_str!("../../../man/man1/dql-version.1")),
    ];
    for (name, section, troff) in PAGES {
        let plain = crate::man_scrub::scrub(troff)
            .unwrap_or_else(|e| panic!("shipped page {name} outside house dialect: {e}"));
        surface
            .man_pages
            .push((name.to_string(), *section, troff.to_string(), plain));
    }
}

fn walk_command(cmd: &clap::Command, parent: Option<&str>, surface: &mut HelpSurface) {
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
        surface.options.push(HelpOption {
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
            summary: arg
                .get_help()
                .map(|s| s.to_string())
                .unwrap_or_default(),
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
fn enrich_option_values(surface: &mut HelpSurface) {
    // -f formats: porcelain/plumbing per PORCELAIN-AND-PLUMBING.md §4.
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
    for d in ["sqlite", "postgres", "postgresql", "mysql", "sqlserver", "duckdb"] {
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
                "sql" | "ast-unresolved" | "ast-resolved" | "ast-refined" | "ast-sql"
                | "cst" => {
                    v.4 = Some("porcelain+semantic-warranty".to_string());
                }
                _ => {}
            }
        }
    }
}

fn seed_envs(surface: &mut HelpSurface) {
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
            "NO_COLOR",
            "Color auto-detection yields no color (no-color.org); explicit --color wins",
            Some("--color"),
        ),
    ];
    for (name, effect, flag) in envs {
        surface.envs.push((
            name.to_string(),
            effect.to_string(),
            flag.map(String::from),
        ));
    }
}

/// The ratified exit-code policy (dql(1) EXIT STATUS): 0/1 + structured
/// stderr records everywhere; 2 usage; dedicated gate flags document
/// their own codes.
fn seed_exit_codes(surface: &mut HelpSurface) {
    let codes: &[(i64, &str, &str)] = &[
        (0, "global", "Success"),
        (
            1,
            "global",
            "Error; a structured record is emitted on stderr (panics included: internal/panic)",
        ),
        (2, "usage", "Command-line usage error (unknown flag or invalid value)"),
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

fn seed_dot_commands(surface: &mut HelpSurface) {
    // The REPL's dot commands are not yet enumerable from code (their
    // dispatch is a match); until they are, seed nothing rather than
    // hand-author a second source that can drift. SYS-HELP-DESIGN.md
    // open item.
    let _ = surface;
}
