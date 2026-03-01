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
