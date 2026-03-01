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
    let (source, color, fail_if_not_formatted) = match command {
        Command::Format {
            source,
            color,
            fail_if_not_formatted,
            ..
        } => (source, color, fail_if_not_formatted),
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
            color,
            *fail_if_not_formatted,
            #[cfg(feature = "repl")]
            highlights,
            #[cfg(feature = "repl")]
            theme,
        );
    }

    // Fall back to external binary
    #[cfg(not(feature = "formatter"))]
    {
        return format_via_shellout(source, *fail_if_not_formatted);
    }
}

/// Format using the linked delightql-formatter library.
#[cfg(feature = "formatter")]
fn format_with_library(
    source: &Option<String>,
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

    // Format
    extern "C" {
        fn tree_sitter_delightql_v2() -> tree_sitter::Language;
    }
    let language = unsafe { tree_sitter_delightql_v2() };
    let config = delightql_formatter::load_config(None);
    let formatted = delightql_formatter::format(&input, &language, &config)?;

    // Check mode: exit 1 if input differs from formatted
    if fail_if_not_formatted {
        if input != formatted {
            std::process::exit(1);
        }
        return Ok(());
    }

    // Apply syntax highlighting if requested
    let use_colors = match color {
        ColorMode::Always => true,
        ColorMode::Never => false,
        ColorMode::Auto => io::stdout().is_terminal(),
    };

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
