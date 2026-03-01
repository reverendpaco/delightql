//! Generic subcommand delegation: try linked library, fall back to external binary.

use anyhow::Result;
use std::process::{Command, Stdio};

/// Shell out to an external binary, forwarding stdin/stdout/stderr transparently.
///
/// `binary_name` is searched on PATH. `args` are the CLI arguments to pass.
/// The external binary's exit code is propagated directly.
pub fn shell_out(binary_name: &str, args: &[&str]) -> Result<()> {
    let status = Command::new(binary_name)
        .args(args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status();

    match status {
        Ok(s) if s.success() => Ok(()),
        Ok(s) => std::process::exit(s.code().unwrap_or(1)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            anyhow::bail!(
                "'{}' not found on PATH.\n\
                 Install it or rebuild dql with the corresponding feature enabled.",
                binary_name,
            )
        }
        Err(e) => anyhow::bail!("Failed to run '{}': {}", binary_name, e),
    }
}
