use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogEntryKind {
    Query,
    DotCommand,
    Error,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionLogEntry {
    pub timestamp: DateTime<Utc>,
    pub input: String,
    pub kind: LogEntryKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_time_ms: Option<f64>,
}

pub struct SessionLog {
    pub started_at: DateTime<Utc>,
    pub cli_args: Vec<String>,
    pub db_path: Option<String>,
    pub entries: Vec<SessionLogEntry>,
}

impl SessionLog {
    pub fn new(cli_args: Vec<String>, db_path: Option<String>) -> Self {
        Self {
            started_at: Utc::now(),
            cli_args,
            db_path,
            entries: Vec::new(),
        }
    }

    pub fn log_query(
        &mut self,
        input: &str,
        sql: Option<String>,
        execution_time_ms: Option<f64>,
        error: Option<String>,
    ) {
        let kind = if error.is_some() {
            LogEntryKind::Error
        } else {
            LogEntryKind::Query
        };
        self.entries.push(SessionLogEntry {
            timestamp: Utc::now(),
            input: input.to_string(),
            kind,
            output: error,
            sql,
            execution_time_ms,
        });
    }

    pub fn log_dot_command(&mut self, input: &str) {
        self.entries.push(SessionLogEntry {
            timestamp: Utc::now(),
            input: input.to_string(),
            kind: LogEntryKind::DotCommand,
            output: None,
            sql: None,
            execution_time_ms: None,
        });
    }
}

#[derive(Debug, Serialize)]
struct DatabaseResource {
    original_path: String,
    archive_path: String,
    role: String,
}

#[derive(Debug, Serialize)]
struct DdlResource {
    original_path: String,
    archive_path: String,
}

/// Extract a filesystem path from a cartridge source_uri.
///
/// Handles:
/// - `file://path/to/file` → `path/to/file`
/// - `pipe://backend/path/to/file` → `path/to/file`
/// - Other schemes (`bootstrap://`, `catalog://`, `sys://`, `embedded://`) → None
fn extract_file_path(uri: &str) -> Option<PathBuf> {
    if let Some(path_str) = uri.strip_prefix("file://") {
        let path = PathBuf::from(path_str);
        if path.exists() {
            return Some(path);
        }
    } else if let Some(rest) = uri.strip_prefix("pipe://") {
        // pipe://backend/path → extract path after the backend component
        if let Some(slash_pos) = rest.find('/') {
            let path_str = &rest[slash_pos + 1..];
            if !path_str.is_empty() {
                let path = PathBuf::from(path_str);
                if path.exists() {
                    return Some(path);
                }
            }
        }
    }
    None
}

/// Gather DDL files and database files from cartridge query results.
///
/// Takes rows with (source_uri, source_type_enum) columns, as returned by
/// `sys::cartridges.cartridge(*) |> (source_uri, source_type_enum)`.
/// source_type_enum: 1 = DDL file, 3 = database (attached or mounted).
pub fn gather_resources_from_rows(
    rows: &[Vec<String>],
    uri_col: usize,
    kind_col: usize,
    db_path: &Option<String>,
) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let mut ddl_files: Vec<PathBuf> = Vec::new();
    let mut db_files: Vec<PathBuf> = Vec::new();

    for row in rows {
        if let (Some(uri), Some(kind_str)) = (row.get(uri_col), row.get(kind_col)) {
            let kind: i32 = kind_str.parse().unwrap_or(0);
            if let Some(path) = extract_file_path(uri) {
                match kind {
                    1 => ddl_files.push(path),
                    3 => db_files.push(path),
                    _ => {}
                }
            }
        }
    }

    // Add the primary database
    if let Some(ref p) = db_path {
        let primary = PathBuf::from(p);
        if primary.exists() && !db_files.iter().any(|f| f == &primary) {
            db_files.insert(0, primary);
        }
    }

    (ddl_files, db_files)
}

pub fn create_bug_tarball(
    title: &str,
    description: &str,
    session_log: &SessionLog,
    ddl_files: Vec<PathBuf>,
    db_files: Vec<PathBuf>,
    db_type: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let archive_name = format!("bug-{}.tgz", title);
    let archive_path = PathBuf::from("/tmp").join(&archive_name);
    let prefix = format!("bug-{}", title);

    let file = std::fs::File::create(&archive_path)?;
    let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    let mut tar = tar::Builder::new(enc);

    let mut db_resources = Vec::new();
    for (i, db_file) in db_files.iter().enumerate() {
        let role = if i == 0 { "primary" } else { "attached" };
        let filename = db_file
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| format!("db_{}.db", i));
        let archive_entry = format!("db/{}", filename);

        db_resources.push(DatabaseResource {
            original_path: db_file.to_string_lossy().to_string(),
            archive_path: archive_entry.clone(),
            role: role.to_string(),
        });

        let tar_path = format!("{}/{}", prefix, archive_entry);
        tar.append_path_with_name(db_file, &tar_path)?;
    }

    let mut ddl_resources = Vec::new();
    for ddl_file in &ddl_files {
        let filename = ddl_file
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown.dql".to_string());
        let archive_entry = format!("ddl/{}", filename);

        ddl_resources.push(DdlResource {
            original_path: ddl_file.to_string_lossy().to_string(),
            archive_path: archive_entry.clone(),
        });

        let tar_path = format!("{}/{}", prefix, archive_entry);
        tar.append_path_with_name(ddl_file, &tar_path)?;
    }

    let entries: Vec<serde_json::Value> = session_log
        .entries
        .iter()
        .map(|e| {
            let mut obj = serde_json::json!({
                "timestamp": e.timestamp.to_rfc3339(),
                "kind": e.kind,
                "input": e.input,
            });
            if let Some(ref sql) = e.sql {
                obj["sql"] = serde_json::json!(sql);
            }
            if let Some(ref output) = e.output {
                obj["output"] = serde_json::json!(output);
            }
            if let Some(ms) = e.execution_time_ms {
                obj["execution_time_ms"] = serde_json::json!(ms);
            }
            obj
        })
        .collect();

    let manifest = serde_json::json!({
        "version": "1.0",
        "title": title,
        "description": description,
        "created_at": Utc::now().to_rfc3339(),
        "session": {
            "started_at": session_log.started_at.to_rfc3339(),
            "cli_args": session_log.cli_args,
            "db_path": session_log.db_path,
            "db_type": db_type,
        },
        "entries": entries,
        "resources": {
            "databases": db_resources,
            "ddl_files": ddl_resources,
        },
    });

    let manifest_json = serde_json::to_string_pretty(&manifest)?;
    let manifest_bytes = manifest_json.as_bytes();

    let mut header = tar::Header::new_gnu();
    header.set_size(manifest_bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();

    let manifest_tar_path = format!("{}/manifest.json", prefix);
    tar.append_data(&mut header, &manifest_tar_path, manifest_bytes)?;

    let enc = tar.into_inner()?;
    enc.finish()?;

    Ok(archive_path.to_string_lossy().to_string())
}
