// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use clap::Parser;
use std::io::{self, IsTerminal, Read};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "dql-fmt", about = "Format DelightQL queries")]
struct Args {
    /// File path or literal DQL string (reads stdin if omitted)
    source: Option<String>,

    /// Path to .dql-format config file
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Print nothing; exit 0 if already formatted, 1 if not,
    /// 2 if the formatter cannot determine (parse error, unhandled
    /// construct). For CI enforcement.
    #[arg(long)]
    fail_if_not_formatted: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Read source
    let source = if let Some(ref s) = args.source {
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

    // Load config. An explicitly named file must exist AND read —
    // missing, permission-denied, and non-UTF-8 all refuse; only the
    // implicitly discovered .dql-format may be quietly absent.
    if let Some(ref p) = args.config {
        if let Err(e) = std::fs::read_to_string(p) {
            anyhow::bail!("--config {}: {e}", p.display());
        }
    }
    let (config, warnings) = delightql_formatter::load_config_report(args.config.as_deref());
    for warning in warnings {
        eprintln!("warning: {warning}");
    }

    let outcome = delightql_formatter::format_outcome(&source, &config)?;

    // Pass-through is safe but loud; "cannot determine" exits 2 so CI
    // can tell a formatter gap (2) from needs-formatting (1).
    if let delightql_formatter::FormatOutcome::PassedThrough { ref reason, .. } = outcome {
        use delightql_formatter::PassReason;
        match reason {
            PassReason::ParseError => {
                eprintln!("warning: input does not parse; returned unchanged");
                // No formatted form of unparseable input exists —
                // "cannot determine" in both modes.
                if !args.fail_if_not_formatted {
                    print!("{}", outcome.text());
                }
                std::process::exit(2);
            }
            PassReason::DefinitionFile => {
                eprintln!(
                    "warning: this is a definition library — dql-fmt speaks the \
                     query form only; returned unchanged"
                );
                if !args.fail_if_not_formatted {
                    print!("{}", outcome.text());
                }
                std::process::exit(2);
            }
            PassReason::UnhandledNode(kind) => eprintln!(
                "warning: formatter does not yet handle node '{kind}'; \
                 input returned unchanged"
            ),
            PassReason::TokenStreamChanged(detail) => eprintln!(
                "warning: formatting would have changed the token stream \
                 ({detail}); input returned unchanged"
            ),
        }
        if args.fail_if_not_formatted {
            std::process::exit(2);
        }
    }
    let formatted = outcome.text();

    if args.fail_if_not_formatted {
        // A missing final newline alone is forgiven — SOURCE may be a
        // literal command-line string, which naturally has none.
        if source != formatted && format!("{source}\n") != formatted {
            std::process::exit(1);
        }
    } else {
        print!("{}", formatted);
    }

    Ok(())
}
