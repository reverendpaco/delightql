// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Format command handler
//!
//! When built with feature = "formatter", uses the linked delightql-formatter library.
//! Otherwise, shells out to `dql-fmt` on PATH.

use crate::args::{CliArgs, ColorMode, Command};
use anyhow::Result;

/// Handle format subcommand.
///
/// Routes: dql format [SOURCE] [--color MODE] [--fail_if_not_formatted]
pub fn handle_format_subcommand(command: &Command, _base_args: &CliArgs) -> Result<()> {
    // Extract fields from Format variant
    let (source, style, color, fail_if_not_formatted) = match command {
        Command::Format {
            source,
            style,
            color,
            fail_if_not_formatted,
            ..
        } => (source, style, color, fail_if_not_formatted),
        _ => unreachable!("handle_format_subcommand called with non-Format command"),
    };

    // Try linked library first
    #[cfg(feature = "formatter")]
    {
        // Also extract repl-gated fields when available
        #[cfg(feature = "repl")]
        let (highlights, theme) = match command {
            Command::Format {
                highlights, theme, ..
            } => (highlights, theme),
            _ => unreachable!(),
        };

        return format_with_library(
            source,
            style,
            color,
            *fail_if_not_formatted,
            #[cfg(feature = "repl")]
            highlights,
            #[cfg(feature = "repl")]
            theme,
        );
    }

    // Fall back to external binary. It cannot honor options the
    // linked formatter resolves (style bundles need the catalog;
    // dql-fmt has no color) — refuse them loudly rather than
    // silently formatting with different semantics per build shape.
    #[cfg(not(feature = "formatter"))]
    {
        if style.is_some() {
            anyhow::bail!(
                "--style requires the linked formatter (feature \"formatter\"); \
                 the dql-fmt fallback cannot resolve style bundles"
            );
        }
        // `never` is honored by plain text; only `always` demands
        // highlighting the fallback cannot produce.
        if matches!(color, ColorMode::Always) {
            anyhow::bail!(
                "--color always requires the linked formatter (feature \"formatter\"); \
                 the dql-fmt fallback prints plain text only"
            );
        }
        return format_via_shellout(source, *fail_if_not_formatted);
    }
}

/// Read one style bundle row from sys::format.bundle and apply its
/// non-NULL columns. The table lives in the session's bootstrap
/// catalog, so no user database is needed.
#[cfg(feature = "formatter")]
fn apply_style_bundle(config: &mut delightql_formatter::FormatConfig, style: &str) -> Result<()> {
    let mut handle = crate::connection::open_handle().map_err(|e| anyhow::anyhow!("{}", e))?;
    let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
    let query = format!("sys::format.bundle(*), bundle = \"{}\"", style);
    let qr = session
        .query(&query)
        .map_err(|e| anyhow::anyhow!("reading style bundle: {}", e))?;
    let fr = session
        .fetch(&qr.handle, 2)
        .map_err(|e| anyhow::anyhow!("reading style bundle: {}", e))?;
    let row = match fr.rows.first() {
        Some(r) => r,
        None => anyhow::bail!(
            "no style bundle named '{style}' in sys::format.bundle \
             (list them: dql query 'sys::format.bundle(*)')"
        ),
    };
    for (col, cell) in qr.columns.iter().zip(row.iter()) {
        if col.name == "bundle" {
            continue;
        }
        // NULL means "inherit" — the knob keeps its current value.
        if let Some(bytes) = cell {
            let value = String::from_utf8_lossy(bytes);
            config
                .apply(&col.name, &value)
                .map_err(|e| anyhow::anyhow!("style bundle '{style}': {e}"))?;
        }
    }
    Ok(())
}

/// Format using the linked delightql-formatter library.
#[cfg(feature = "formatter")]
fn format_with_library(
    source: &Option<String>,
    style: &Option<String>,
    color: &ColorMode,
    fail_if_not_formatted: bool,
    #[cfg(feature = "repl")] highlights: &Option<std::path::PathBuf>,
    #[cfg(feature = "repl")] theme: &Option<std::path::PathBuf>,
) -> Result<()> {
    use std::io::{self, IsTerminal, Read};

    // Determine source
    let input = if let Some(ref s) = source {
        if std::path::Path::new(s).exists() {
            std::fs::read_to_string(s)?
        } else {
            s.clone()
        }
    } else if !io::stdin().is_terminal() {
        let mut buffer = String::new();
        io::stdin().read_to_string(&mut buffer)?;
        buffer
    } else {
        anyhow::bail!("Must provide source code, file path, or pipe input to format");
    };

    // Resolution order: frozen defaults, then the selected style
    // bundle (sys::format.bundle row, NULL = inherit), then the
    // .dql-format file overriding individual knobs.
    let mut config = delightql_formatter::FormatConfig::default();
    if let Some(style_name) = style {
        apply_style_bundle(&mut config, style_name)?;
    }
    for warning in delightql_formatter::apply_config_file(&mut config, None) {
        crate::client::incident::warning("format", crate::client::incident::hierarchy::FORMAT, warning.to_string());
    }
    let outcome = delightql_formatter::format_outcome(&input, &config)?;

    // Pass-through is safe but must be LOUD: the formatter could not
    // fully determine the input and returned it unchanged. "Cannot
    // determine" must not report "formatted": exit 2 so CI can tell a
    // formatter gap (2) from needs-formatting (1).
    if let delightql_formatter::FormatOutcome::PassedThrough { ref reason, .. } = outcome {
        use delightql_formatter::PassReason;
        match reason {
            PassReason::DefinitionFile => {
                crate::client::incident::warning(
                    "format",
                    crate::client::incident::hierarchy::FORMAT,
                    "this is a definition library — `dql format` speaks the \
                     query grammar only and cannot format rule definitions \
                     yet; returned unchanged"
                        .to_string(),
                );
                if !fail_if_not_formatted {
                    print!("{}", outcome.text());
                }
                {
                crate::client::exit::finish(None, 2);
                crate::client::exit::announce();
                std::process::exit(2);
            }
            }
            PassReason::ParseError => {
                // THE TOOL'S ACCOMMODATION, not a second semantics. `dql
                // format` names the utility entrance; a submission the
                // canonical entrance reads cleanly is a definition library,
                // which is a formatter limitation rather than a defect in the
                // input. Asking is how the warning gets worded — the entrance
                // was already chosen and this cannot change it.
                let is_definition_file = !delightql_cst::Parser::new()
                    .parse_definition_file(&input)
                    .has_defects();
                if is_definition_file {
                    crate::client::incident::warning(
                        "format",
                        crate::client::incident::hierarchy::FORMAT,
                        "this is a definition library — `dql format` speaks the \
                         query grammar only and cannot format rule definitions \
                         yet; returned unchanged"
                            .to_string(),
                    );
                } else {
                    crate::client::incident::warning("format", crate::client::incident::hierarchy::FORMAT, "input does not parse; returned unchanged".to_string());
                }
                // A parse error is "cannot determine" in BOTH modes —
                // there is no formatted form of unparseable input.
                if !fail_if_not_formatted {
                    print!("{}", outcome.text());
                }
                {
                crate::client::exit::finish(None, 2);
                crate::client::exit::announce();
                std::process::exit(2);
            }
            }
            PassReason::UnhandledNode(kind) => crate::client::incident::warning(
                "format",
                crate::client::incident::hierarchy::FORMAT,
                format!("formatter does not yet handle node '{kind}'; input returned unchanged"),
            ),
            PassReason::TokenStreamChanged(detail) => crate::client::incident::warning(
                "format",
                crate::client::incident::hierarchy::FORMAT,
                format!(
                    "formatting would have changed the token stream ({detail}); \
                     input returned unchanged"
                ),
            ),
        }
        if fail_if_not_formatted {
            {
                crate::client::exit::finish(None, 2);
                crate::client::exit::announce();
                std::process::exit(2);
            }
        }
    }
    let formatted = outcome.text().to_string();

    // Check mode: exit 1 if input differs from formatted. A missing
    // final newline alone is forgiven — SOURCE may be a literal
    // command-line string, which naturally has none.
    if fail_if_not_formatted {
        if input != formatted && format!("{input}\n") != formatted {
            {
                crate::client::exit::finish(None, 1);
                crate::client::exit::announce();
                std::process::exit(1);
            }
        }
        return Ok(());
    }

    // Apply syntax highlighting if requested
    // NO_COLOR convention (no-color.org): when set, auto-detection
    // yields no color; the explicit flag always wins (R2.5).
    let use_colors = match color {
        ColorMode::Always => true,
        ColorMode::Never => false,
        ColorMode::Auto => io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none(),
    };

    // Highlighting lives behind "repl"; without it, `always` is a
    // promise this build cannot keep — refuse rather than silently
    // print plain text. (`auto` detects no capability; `never` is
    // honored by plain text.)
    #[cfg(not(feature = "repl"))]
    if matches!(color, ColorMode::Always) {
        anyhow::bail!(
            "--color always requires the \"repl\" feature (syntax highlighting); \
             this build prints plain text only"
        );
    }

    let output = if use_colors {
        #[cfg(feature = "repl")]
        {
            crate::repl::syntax_highlighter::highlight_text(
                &formatted,
                highlights.as_deref(),
                theme.as_deref(),
            )
        }
        #[cfg(not(feature = "repl"))]
        {
            formatted
        }
    } else {
        formatted
    };

    print!("{}", output);
    Ok(())
}

/// Format by shelling out to `dql-fmt` on PATH.
#[cfg(not(feature = "formatter"))]
fn format_via_shellout(source: &Option<String>, fail_if_not_formatted: bool) -> Result<()> {
    let mut args: Vec<&str> = Vec::new();

    if fail_if_not_formatted {
        args.push("--fail-if-not-formatted");
    }

    if let Some(ref s) = source {
        args.push(s);
    }

    super::delegate::shell_out("dql-fmt", &args)
}
