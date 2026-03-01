/// File and stdin execution module
use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

use crate::args::Stage;
use crate::connection::ConnectionManager;
use crate::exec_ng::ResultMetadata;
use crate::output_format::OutputFormat;

/// Execute a query from a file
pub fn execute_file(
    file_path: &Path,
    db_path: Option<String>,
    output_format: OutputFormat,
    target_stage: Option<Stage>,
    interactive: bool,
    no_headers: bool,
) -> Result<Option<String>> {
    let source_code = {
        use std::io::Read;
        let mut file = fs::File::open(file_path)
            .with_context(|| format!("Failed to open file '{}'", file_path.display()))?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .with_context(|| format!("Failed to read file '{}'", file_path.display()))?;
        contents
    };

    if interactive {
        return Ok(Some(source_code));
    }

    execute_query(
        &source_code,
        db_path,
        output_format,
        target_stage,
        no_headers,
    )?;
    Ok(None)
}

/// Execute a query string
pub fn execute_query(
    source_code: &str,
    db_path: Option<String>,
    output_format: OutputFormat,
    target_stage: Option<Stage>,
    no_headers: bool,
) -> Result<Option<ResultMetadata>> {
    let conn = if let Some(ref path) = db_path {
        ConnectionManager::new_file(path)?
    } else {
        ConnectionManager::new_memory()?
    };

    let mut handle = conn.open_handle()?;

    let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;

    // mount! the user database as "main" if specified
    if let Some(ref path) = db_path {
        crate::exec_ng::run_dql_query(&format!("mount!(\"{}\", \"main\")", path), &mut *session)?;
    }

    crate::exec_ng::execute_query(
        source_code,
        &mut *session,
        target_stage,
        output_format,
        no_headers,
        false,
        false,
    )
}
