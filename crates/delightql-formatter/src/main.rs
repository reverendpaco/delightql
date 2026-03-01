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

    /// Exit 1 if input is not already formatted (for CI enforcement)
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

    // Load config
    let config = delightql_formatter::load_config(args.config.as_deref());

    // Get language
    let language = delightql_formatter::language();

    // Format
    let formatted = delightql_formatter::format(&source, &language, &config)?;

    if args.fail_if_not_formatted {
        // Exit 1 if input differs from formatted output
        if source != formatted {
            std::process::exit(1);
        }
    } else {
        print!("{}", formatted);
    }

    Ok(())
}
