// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PipeError {
    #[error("failed to spawn coprocess '{binary}': {source}")]
    SpawnFailed {
        binary: String,
        source: std::io::Error,
    },

    #[error("coprocess stdin unavailable")]
    StdinUnavailable,

    #[error("coprocess stdout unavailable")]
    StdoutUnavailable,

    #[error("coprocess stderr unavailable")]
    StderrUnavailable,

    #[error("I/O error communicating with coprocess: {0}")]
    Io(#[from] std::io::Error),

    #[error("CSV parse error: {0}")]
    CsvParse(#[from] csv::Error),

    #[error("frame timeout: end sentinel not received")]
    FrameTimeout,

    #[error("coprocess exited unexpectedly{}", if .stderr.is_empty() { String::new() } else { format!("\n{}", .stderr) })]
    ProcessExited { stderr: String },

    #[error("pipe query failed: {0}")]
    QueryFailed(String),
}

pub type Result<T> = std::result::Result<T, PipeError>;

/// The coprocess road's refusals as DelightQL diagnostics, at the boundary
/// where a `PipeError` leaves this crate: the process itself (spawn, pipes,
/// exit, timeout), the tool's own refusal of a statement, or output the
/// profile could not read.
pub fn diagnostic(context: &str, error: PipeError) -> delightql_types::DelightQLError {
    use delightql_types::diagnostic::Siso;
    let message = format!("{context}: {error}");
    match error {
        PipeError::SpawnFailed { .. }
        | PipeError::StdinUnavailable
        | PipeError::StdoutUnavailable
        | PipeError::StderrUnavailable
        | PipeError::Io(_)
        | PipeError::FrameTimeout
        | PipeError::ProcessExited { .. } => Siso::Process { message }.into(),
        PipeError::CsvParse(_) => Siso::Output { message }.into(),
        PipeError::QueryFailed(_) => Siso::Query { message }.into(),
    }
}
